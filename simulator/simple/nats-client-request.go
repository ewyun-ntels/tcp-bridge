package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type requestPayload struct {
	CodeType       string `json:"code-type"`
	SystemID       string `json:"sys-id"`
	BranchName     string `json:"branch-name"`
	TID            string `json:"tid"`
	EventTimestamp string `json:"event-timestamp"`
	MDN            string `json:"mdn"`
	DeviceType     string `json:"device-type"`
	ProductType    string `json:"product-type"`
}

var (
	natsURL         = flag.String("url", "nats://10.255.254.22:30422", "NATS server URL")
	requestSubject  = flag.String("subject", "tcp.subs.change", "NATS subject to request")
	requestInterval = flag.Duration("interval", 3*time.Second, "Interval between NATS requests")
	requestTimeout  = flag.Duration("timeout", 10*time.Second, "Request timeout")
	requestCount    = flag.Int("count", 0, "Number of requests to send (0 = unlimited)")
	codeType        = flag.String("code-type", "06", "Request body field: code-type")
	systemID        = flag.String("sys-id", "PG01", "Request body field: sys-id")
	branchName      = flag.String("branch-name", "SS", "Request body field: branch-name")
	tid             = flag.String("tid", "UPM01-201710139999999999", "Request body field: tid")
	eventTimestamp  = flag.String("event-timestamp", "20171013235959000", "Request body field: event-timestamp")
	mdn             = flag.String("mdn", "01012345678", "Request body field: mdn")
	deviceType      = flag.String("device-type", "S", "Request body field: device-type")
	productType     = flag.String("product-type", "03", "Request body field: product-type")
)

func prettyJSON(data []byte) string {
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, data, "", "  "); err != nil {
		return string(data)
	}
	return pretty.String()
}

func main() {
	flag.Parse()

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	fmt.Println("=== NATS Request Client ===")
	fmt.Printf("connected=%s subject=%s interval=%v timeout=%v count=%d\n",
		*natsURL, *requestSubject, *requestInterval, *requestTimeout, *requestCount)

	sent := 0

	for {
		if *requestCount > 0 && sent >= *requestCount {
			fmt.Printf("sent %d requests, exiting\n", sent)
			return
		}

		payload, err := json.Marshal(requestPayload{
			CodeType:       *codeType,
			SystemID:       *systemID,
			BranchName:     *branchName,
			TID:            *tid,
			EventTimestamp: *eventTimestamp,
			MDN:            *mdn,
			DeviceType:     *deviceType,
			ProductType:    *productType,
		})
		if err != nil {
			log.Fatalf("failed to marshal request: %v", err)
		}

		fmt.Printf("\n[%s] REQ subject=%s\n%s\n",
			time.Now().Format("15:04:05.000"), *requestSubject, prettyJSON(payload))

		ctx, cancel := context.WithTimeout(context.Background(), *requestTimeout)
		msg, err := nc.RequestWithContext(ctx, *requestSubject, payload)
		cancel()
		if err != nil {
			fmt.Printf("[%s] RESP error: %v\n", time.Now().Format("15:04:05.000"), err)
		} else {
			fmt.Printf("[%s] RESP subject=%s\n%s\n",
				time.Now().Format("15:04:05.000"), msg.Subject, prettyJSON(msg.Data))
		}

		sent++

		if *requestInterval <= 0 {
			return
		}
		time.Sleep(*requestInterval)
	}
}
