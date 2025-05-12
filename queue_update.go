package asynq

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
)

type QueueUpdateListener struct {
	logger          *log.Logger
	broker          base.Broker
	done            chan struct{}
	listenToUpdates bool

	server *Server
}

func NewQueueUpdateListener(logger *log.Logger, broker base.Broker, server *Server, listenToUpdates bool) *QueueUpdateListener {
	return &QueueUpdateListener{
		logger:          logger,
		broker:          broker,
		done:            make(chan struct{}),
		listenToUpdates: listenToUpdates,
		server:          server,
	}
}

func (l *QueueUpdateListener) shutdown() {
	if l.listenToUpdates {
		l.logger.Debug("Queue update listener shutting down...")
		l.done <- struct{}{}
	}
}

func (l *QueueUpdateListener) start(wg *sync.WaitGroup) {
	if l.listenToUpdates {

		wg.Add(1)
		go func() {
			defer wg.Done()

			client := l.broker.(*rdb.RDB).Client()

			pubsub := client.Subscribe(context.Background(), "asynq:queue:updates")
			defer pubsub.Close()

			_, err := pubsub.Receive(context.Background())
			if err != nil {
				l.logger.Errorf("Failed to subscribe to queue updates: %v", err)
				return
			}

			ch := pubsub.Channel()
			for {
				select {
				case <-l.done:
					l.logger.Debug("Queue update listener done")
					return

				case msg, ok := <-ch:
					if !ok {
						return
					}

					parts := strings.Split(msg.Payload, ":")
					if len(parts) != 3 {
						l.logger.Errorf("Invalid queue update format: %s", msg.Payload)
						continue
					}

					qname := parts[0]
					priority, err1 := strconv.Atoi(parts[1])
					concurrency, err2 := strconv.Atoi(parts[2])

					if err1 != nil || err2 != nil {
						l.logger.Errorf("Invalid queue update values: %s", msg.Payload)
						continue
					}

					l.server.mu.Lock()
					if _, exists := l.server.queues[qname]; !exists {
						l.server.queues[qname] = priority
						l.server.broker.SetQueueConcurrency(qname, concurrency)
						l.server.updateComponentsWithNewQueue(qname)
						l.logger.Infof("Added new queue from Redis event: %s with priority %d", qname, priority)
					}
					l.server.mu.Unlock()
				}
			}
		}()
	}
}
