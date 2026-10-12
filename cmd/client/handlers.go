package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(playingState routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(playingState)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, amqpChannel *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(armyMove gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		move := gs.HandleMove(armyMove)
		switch move {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(amqpChannel, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+armyMove.Player.Username, gamelogic.RecognitionOfWar{
				Attacker: armyMove.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			fallthrough
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(dw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print(">")

		warOutcome, winner, loser := gs.HandleWar(dw)

		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(channel, gs.Player.Username, msg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(channel, gs.Player.Username, msg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := publishGameLog(channel, gs.Player.Username, msg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Printf("Error handling war outcome")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(channel *amqp.Channel, username string, message string) error {
	log := routing.GameLog{
		CurrentTime: time.Now(),
		Username:    username,
		Message:     message,
	}
	err := pubsub.PublishGob(channel, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, log)

	return err
}
