package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

type inboundRequest struct {
	RequestID int    `json:"request_id"`
	Action    string `json:"action"`
}

type outboundResponse struct {
	RequestID   int                    `json:"request_id"`
	Status      string                 `json:"status"`
	Data        map[string]interface{} `json:"data"`
	Timestamp   int64                  `json:"timestamp"`
	ProcessedBy string                 `json:"processed_by"`
}

var (
	natsURL    = flag.String("url", "nats://localhost:4222", "NATS server URL")
	subject    = flag.String("subject", "tcp.subs.info", "NATS subject to subscribe")
	serviceTag = flag.String("name", "nats-client-responder", "Responder name")
)

func main() {
	flag.Parse()

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	fmt.Println("=== NATS Responder Client ===")
	fmt.Printf("connected=%s subject=%s name=%s\n", *natsURL, *subject, *serviceTag)

	_, err = nc.Subscribe(*subject, func(msg *nats.Msg) {
		fmt.Printf("\n[%s] received subject=%s reply=%s payload=%s\n",
			time.Now().Format("15:04:05.000"), msg.Subject, msg.Reply, string(msg.Data))

		var req inboundRequest
		_ = json.Unmarshal(msg.Data, &req)

		responseBytes, err := json.Marshal(outboundResponse{
			RequestID: req.RequestID,
			Status:    "success",
			Data: map[string]interface{}{
				"echo_subject": msg.Subject,
				"echo_action":  req.Action,
				"received_raw": string(msg.Data),
			},
			Timestamp:   time.Now().Unix(),
			ProcessedBy: *serviceTag,
		})
		if err != nil {
			fmt.Printf("[%s] failed to marshal response: %v\n", time.Now().Format("15:04:05.000"), err)
			return
		}

		if msg.Reply == "" {
			fmt.Printf("[%s] no reply subject, skipping response\n", time.Now().Format("15:04:05.000"))
			return
		}

		if err := msg.Respond(responseBytes); err != nil {
			fmt.Printf("[%s] failed to respond: %v\n", time.Now().Format("15:04:05.000"), err)
			return
		}

		fmt.Printf("[%s] responded payload=%s\n", time.Now().Format("15:04:05.000"), string(responseBytes))
	})
	if err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
