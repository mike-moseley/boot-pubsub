package main

import (
	"fmt"
	"log"
	"strconv"

	// "os"
	// "os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	ampq "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionURL := "amqp://guest:guest@localhost:5672/"
	connection, err := ampq.Dial(connectionURL)
	if err != nil {
		log.Fatalf("Could not create connection: %v", err)
	}
	defer connection.Close()
	fmt.Println("Connection successful")

	publishChannel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Could not create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Could not get user name: %v", err)
	}

	state := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.QueueTransient, handlerPause(state))
	if err != nil {
		log.Fatalf("Could not subscribe: %v", err)
	}
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.QueueTransient, handlerMove(state, publishChannel))
	if err != nil {
		log.Fatalf("Could not subscribe: %v", err)
	}
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix+".*", pubsub.QueueDurable, handlerWar(state, publishChannel))
	if err != nil {
		log.Fatalf("Could not subscribe: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			fmt.Println("Please enter a command")
			continue
		}
		switch words[0] {
		case "spawn":
			{
				err = state.CommandSpawn(words)
				if err != nil {
					fmt.Println("Invalid spawn")
					continue
				}
			}
		case "move":
			{
				move, err := state.CommandMove(words)
				if err != nil {
					fmt.Println("Invalid move")
					continue
				}
				err = pubsub.PublishJSON(publishChannel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+move.Player.Username, move)
				if err != nil {
					fmt.Printf("Error publishing: %v", err)
					continue
				}
				fmt.Printf("Move worked: %v\n", move)
			}
		case "status":
			{
				state.CommandStatus()
			}
		case "help":
			{
				gamelogic.PrintClientHelp()
			}
		case "spam":
			{
				if len(words) != 2 {
					fmt.Printf("Must provide a number for spam!")
				}
				count, err := strconv.Atoi(words[1])
				if err != nil {
					fmt.Printf("Error processing spam count: %v", err)
				}
				for i := 0; i < count; i++ {
					badLog := gamelogic.GetMaliciousLog()
					pubsub.PublishGob(publishChannel, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, badLog)
				}
			}
		case "quit":
			{
				gamelogic.PrintQuit()
				return
			}
		default:
			{
				fmt.Println("Unknown command")
			}
		}
	}
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan
}
