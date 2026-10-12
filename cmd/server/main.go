package main

import (
	"fmt"
	// "os"
	// "os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	ampq "github.com/rabbitmq/amqp091-go"
)

func main() {
	gamelogic.PrintServerHelp()
	fmt.Println("Starting Peril server...")
	connectionURL := "amqp://guest:guest@localhost:5672/"
	connection, err := ampq.Dial(connectionURL)
	if err != nil {
		return
	}
	defer connection.Close()
	fmt.Println("Connection successful")

	connCh, err := connection.Channel()
	if err != nil {
		return
	}

	channel, queue, err := pubsub.DeclareAndBind(connection, "peril_topic", "game_logs", "game_logs.*", pubsub.QueueDurable)
	if err != nil {
		return
	}
	defer channel.Close()
	_ = queue

	pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.QueueDurable, handlerLogs)
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			fmt.Println("Please enter a command")
			continue
		}
		switch words[0] {
		case "pause":
			{
				fmt.Println("Sending pause message")
				err := pubsub.PublishJSON(connCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
				if err != nil {
					return
				}
			}
		case "resume":
			{
				fmt.Println("Sending resume message")
				err := pubsub.PublishJSON(connCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
				if err != nil {
					return
				}
			}
		case "quit":
			{
				fmt.Println("Quitting")
				return
			}
		default:
			{
				fmt.Println("Unknown command")
			}
		}
	}
}
