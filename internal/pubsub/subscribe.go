package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func marshalDeliveries[T any](deliveries <-chan amqp.Delivery, handler func(T) AckType, unmarshaller func([]byte) (T, error)) error {
	for msg := range deliveries {
		gen, err := unmarshaller(msg.Body)
		if err != nil {
			println(err)
		}
		ack := handler(gen)
		switch ack {
		case Ack:
			{
				err = msg.Ack(false)
				if err != nil {
					fmt.Printf("Error acknowledging: %v\n", err)
					continue
				}
				fmt.Println("Acknowledged")
			}
		case NackRequeue:
			{
				err = msg.Nack(false, true)
				if err != nil {
					fmt.Printf("Error negative acknowledging, requeue: %v\n", err)
					continue
				}
				fmt.Println("Negative Acknowledged, requeue")

			}
		case NackDiscard:
			{
				err = msg.Nack(false, false)
				if err != nil {
					fmt.Printf("Error negative acknowledging, discard: %v\n", err)
					continue
				}
				fmt.Println("Negative Acknowledged, discard")
			}
		}
	}
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	channel.Qos(10, 0, true)
	deliveries, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go marshalDeliveries(deliveries, handler, func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	})
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	deliveries, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go marshalDeliveries(deliveries, handler, func(data []byte) (T, error) {
		buffer := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buffer)
		var target T
		err := dec.Decode(&target)
		return target, err
	})
	return nil
}
