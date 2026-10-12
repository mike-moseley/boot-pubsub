package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	QueueDurable SimpleQueueType = iota
	QueueTransient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	valJSON, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Error marshaling JSON: %w", err)
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: valJSON})
	if err != nil {
		return fmt.Errorf("Error marshaling JSON: %w", err)
	}
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(val)
	if err != nil {
		return fmt.Errorf("Error encoding gob: %w", err)
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/gob", Body: data.Bytes()})
	if err != nil {
		return fmt.Errorf("Error encoding gob: %w", err)
	}
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	var durable, autoDelete, exclusive bool
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Error creating channel: %w", err)
	}
	if queueType == QueueTransient {
		autoDelete = true
		exclusive = true
		durable = false
	}
	if queueType == QueueDurable {
		durable = true
		autoDelete = false
		exclusive = false
	}

	table := make(map[string]any)
	table["x-dead-letter-exchange"] = "peril_dlx"
	queue, err := channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, table)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Error declaring queue: %w", err)
	}
	channel.QueueBind(queueName, key, exchange, false, nil)
	return channel, queue, nil
}
