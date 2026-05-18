package messages

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQPublisher struct {
	addr  string
	queue string

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel

	closeOnce sync.Once
	closed    chan struct{}
}

func NewRabbitMQPublisher(addr, queue string) (*RabbitMQPublisher, error) {
	p := &RabbitMQPublisher{
		addr:   addr,
		queue:  queue,
		closed: make(chan struct{}),
	}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *RabbitMQPublisher) connect() error {
	conn, err := amqp.Dial(p.addr)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	if _, err := ch.QueueDeclare(
		p.queue,
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

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return err
	}

	p.mu.Lock()
	p.conn = conn
	p.ch = ch
	p.mu.Unlock()

	return nil
}

func (p *RabbitMQPublisher) Publish(ctx context.Context, body []byte) error {
	select {
	case <-p.closed:
		return amqp.ErrClosed
	default:
	}

	err := p.publish(ctx, body)
	if err == nil {
		return nil
	}

	if isConnError(err) {
		if reconnErr := p.reconnect(ctx); reconnErr != nil {
			return reconnErr
		}
		return p.publish(ctx, body)
	}

	return err
}

func (p *RabbitMQPublisher) publish(ctx context.Context, body []byte) error {
	p.mu.Lock()
	ch := p.ch
	p.mu.Unlock()

	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		p.queue,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-confirmation.Done():
		if !confirmation.Acked() {
			return ErrPublishNacked
		}
	}

	return nil
}

func (p *RabbitMQPublisher) reconnect(ctx context.Context) error {
	delay := defaultReconnectDelay
	for {
		err := p.connect()
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.closed:
			return amqp.ErrClosed
		case <-time.After(delay):
			delay = min(delay*2, maxReconnectDelay)
		}
	}
}

func (p *RabbitMQPublisher) Close() error {
	var closeErr error
	p.closeOnce.Do(func() {
		close(p.closed)

		p.mu.Lock()
		defer p.mu.Unlock()

		if p.ch != nil {
			if err := p.ch.Close(); err != nil {
				closeErr = err
			}
		}
		if p.conn != nil {
			if err := p.conn.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

func isConnError(err error) bool {
	if err == nil {
		return false
	}
	amqpErr, ok := err.(*amqp.Error)
	if !ok {
		return true
	}
	return amqpErr.Code >= 300 && amqpErr.Code < 400
}

var ErrPublishNacked = &publishError{"broker nacked the message"}

type publishError struct{ msg string }

func (e *publishError) Error() string { return e.msg }
