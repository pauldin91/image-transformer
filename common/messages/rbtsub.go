package messages

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultReconnectDelay         = 5 * time.Second
	maxReconnectDelay             = 60 * time.Second
	defaultPrefetch               = 3
	defaultRequeueMessagesOnError = true
)

type RabbitMQSubscriber struct {
	addr                   string
	queue                  string
	prefetchCount          int
	requeueMessagesOnError bool

	mu      sync.RWMutex
	conn    *amqp.Connection
	ch      *amqp.Channel
	handler func(body []byte) error

	closeOnce sync.Once
	closed    chan struct{}
}

func NewRabbitMQSubscriber(
	addr, queue string,
	handler func(body []byte) error,
) (*RabbitMQSubscriber, error) {
	if handler == nil {
		handler = func([]byte) error { return nil }
	}

	s := &RabbitMQSubscriber{
		addr:                   addr,
		queue:                  queue,
		prefetchCount:          defaultPrefetch,
		requeueMessagesOnError: defaultRequeueMessagesOnError,
		handler:                handler,
		closed:                 make(chan struct{}),
	}

	if err := s.connect(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *RabbitMQSubscriber) connect() error {
	conn, err := amqp.Dial(s.addr)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	if _, err := ch.QueueDeclare(
		s.queue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return err
	}

	if err := ch.Qos(s.prefetchCount, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return err
	}

	s.mu.Lock()
	s.conn = conn
	s.ch = ch
	s.mu.Unlock()

	return nil
}

func (s *RabbitMQSubscriber) Start(ctx context.Context) error {
	delay := defaultReconnectDelay

	for {
		_ = s.consume(ctx)

		if ctx.Err() != nil {
			return ctx.Err()
		}

		select {
		case <-s.closed:
			return nil
		default:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		if reconnErr := s.connect(); reconnErr != nil {
			delay = min(delay*2, maxReconnectDelay)
			continue
		}
		delay = defaultReconnectDelay
	}
}

func (s *RabbitMQSubscriber) consume(ctx context.Context) error {
	s.mu.RLock()
	ch := s.ch
	s.mu.RUnlock()

	msgs, err := ch.Consume(
		s.queue,
		"",
		false, // manual ACK
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, s.prefetchCount)

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()

		case <-s.closed:
			wg.Wait()
			return nil

		case d, ok := <-msgs:
			if !ok {
				wg.Wait()
				return amqp.ErrClosed
			}

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			}

			wg.Add(1)
			go func(d amqp.Delivery) {
				defer func() {
					<-sem
					wg.Done()
				}()

				s.mu.RLock()
				h := s.handler
				s.mu.RUnlock()

				if err := h(d.Body); err != nil {
					d.Nack(false, s.requeueMessagesOnError)
					return
				}
				d.Ack(false)
			}(d)
		}
	}
}

func (s *RabbitMQSubscriber) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		close(s.closed)

		s.mu.Lock()
		defer s.mu.Unlock()

		if s.ch != nil {
			if err := s.ch.Close(); err != nil {
				closeErr = err
			}
		}
		if s.conn != nil {
			if err := s.conn.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
