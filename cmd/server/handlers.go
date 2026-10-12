package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLogs(logs routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	gamelogic.WriteLog(logs)
	return pubsub.Ack
}
