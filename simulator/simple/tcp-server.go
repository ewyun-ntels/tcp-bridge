package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	msgTypeHelloRequest  = 0x01
	msgTypeHelloResponse = 0x02
	msgTypePingRequest   = 0x03
	msgTypePingResponse  = 0x04
)

type frame struct {
	Type    uint8
	Length  uint16
	TID     uint32
	Payload []byte
}

type handshakeRequest struct {
	SysID      string `json:"sys-id"`
	BranchName string `json:"branch-name"`
}

type keyListEntry struct {
	Num    int    `json:"num"`
	KeyEnc string `json:"key_enc"`
	IVEnc  string `json:"iv_enc"`
	Salt   string `json:"salt"`
}

type handshakeResponse struct {
	SysID        string         `json:"sys-id"`
	Code         string         `json:"code"`
	PingInterval int            `json:"ping-interval"`
	KeyList      []keyListEntry `json:"keyList"`
	Cause        string         `json:"cause"`
}

type requestPayload struct {
	MessageType string `json:"message_type,omitempty"`
	RequestID   int    `json:"request_id"`
	Action      string `json:"action"`
	UserID      int    `json:"user_id,omitempty"`
	OrderID     int    `json:"order_id,omitempty"`
	Timestamp   int64  `json:"timestamp"`
}

type responsePayload struct {
	RequestID   int                    `json:"request_id"`
	Status      string                 `json:"status"`
	Data        map[string]interface{} `json:"data"`
	Timestamp   int64                  `json:"timestamp"`
	ProcessedBy string                 `json:"processed_by"`
}

type connectionState struct {
	conn    net.Conn
	writeMu sync.Mutex
}

var (
	listenPort      = flag.String("port", "8000", "TCP server listen port")
	requestInterval = flag.Duration("request-interval", 3*time.Second, "Interval between TCP requests")
	requestCount    = flag.Int("request-count", 0, "Number of requests to send (0 = unlimited)")
	requestType     = flag.String("request-type", "07", "Request frame type in hex (default 07)")
)

func ts() string {
	return time.Now().Format("15:04:05.000")
}

func main() {
	flag.Parse()

	reqType, err := parseHexByte(*requestType)
	if err != nil {
		log.Fatalf("invalid -request-type value %q: %v", *requestType, err)
	}

	fmt.Println("=== TCP Simulator Server ===")
	fmt.Printf("[%s] configuration: port=%s request_interval=%v request_count=%d request_type=0x%02X\n",
		ts(), *listenPort, *requestInterval, *requestCount, reqType)

	listener, err := net.Listen("tcp", ":"+*listenPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", *listenPort, err)
	}
	defer listener.Close()

	fmt.Printf("[%s] listening on :%s\n", ts(), *listenPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("[%s] accept failed: %v\n", ts(), err)
			continue
		}
		fmt.Printf("[%s] tcp-bridge connected from %s\n", ts(), conn.RemoteAddr())
		go handleConnection(conn, reqType)
	}
}

func handleConnection(conn net.Conn, reqType uint8) {
	defer conn.Close()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	state := &connectionState{conn: conn}

	if err := performHandshake(state); err != nil {
		fmt.Printf("[%s] handshake failed: %v\n", ts(), err)
		return
	}

	done := make(chan struct{})
	go sendRequestsLoop(state, reqType, done)

	if err := readLoop(state, done); err != nil {
		fmt.Printf("[%s] connection closed: %v\n", ts(), err)
	}
}

func performHandshake(state *connectionState) error {
	fmt.Printf("[%s] waiting for HELLO handshake\n", ts())

	helloFrame, err := readFrame(state.conn)
	if err != nil {
		return fmt.Errorf("failed to read HELLO: %w", err)
	}
	if helloFrame.Type != msgTypeHelloRequest {
		return fmt.Errorf("expected HELLO frame 0x%02X, got 0x%02X", msgTypeHelloRequest, helloFrame.Type)
	}

	var hello handshakeRequest
	if len(helloFrame.Payload) > 0 {
		if err := json.Unmarshal(helloFrame.Payload, &hello); err != nil {
			return fmt.Errorf("failed to parse HELLO payload: %w", err)
		}
	}

	fmt.Printf("[%s] received HELLO tid=%d sys-id=%s branch-name=%s\n", ts(), helloFrame.TID, hello.SysID, hello.BranchName)
	sysID := *listenPort
	if addr, ok := state.conn.LocalAddr().(*net.TCPAddr); ok && addr.Port > 0 {
		sysID = fmt.Sprintf("%d", addr.Port)
	}

	ackPayload, err := json.Marshal(handshakeResponse{
		SysID: sysID,
		// tcp-bridge currently parses ACK code as a string.
		Code:         "200",
		PingInterval: 30,
		KeyList: []keyListEntry{
			{
				Num:    1,
				KeyEnc: "test-key-enc-001",
				IVEnc:  "test-iv-enc-001",
				Salt:   "test-salt-001",
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to marshal ACK: %w", err)
	}

	if err := writeFrame(state, &frame{
		Type:    msgTypeHelloResponse,
		Length:  uint16(len(ackPayload)),
		TID:     helloFrame.TID,
		Payload: ackPayload,
	}, "SENDING ACK"); err != nil {
		return fmt.Errorf("failed to send ACK: %w", err)
	}

	fmt.Printf("[%s] handshake completed\n", ts())
	return nil
}

func sendRequestsLoop(state *connectionState, reqType uint8, done <-chan struct{}) {
	if *requestInterval <= 0 {
		fmt.Printf("[%s] request sender disabled because interval=%v\n", ts(), *requestInterval)
		return
	}

	ticker := time.NewTicker(*requestInterval)
	defer ticker.Stop()

	requestID := 1
	tid := uint32(1000)
	sent := 0

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if *requestCount > 0 && sent >= *requestCount {
				fmt.Printf("[%s] request sender stopped after %d messages\n", ts(), sent)
				return
			}

			req := requestPayload{
				MessageType: "subscriber",
				RequestID:   requestID,
				Action:      "get_subscriber_info",
				UserID:      12345,
				Timestamp:   time.Now().Unix(),
			}

			payload, err := json.Marshal(req)
			if err != nil {
				fmt.Printf("[%s] failed to marshal request: %v\n", ts(), err)
				continue
			}

			if err := writeFrame(state, &frame{
				Type:    reqType,
				Length:  uint16(len(payload)),
				TID:     tid,
				Payload: payload,
			}, "SENDING REQUEST"); err != nil {
				fmt.Printf("[%s] failed to send request tid=%d: %v\n", ts(), tid, err)
				return
			}

			requestID++
			tid++
			sent++
		}
	}
}

func readLoop(state *connectionState, done chan struct{}) error {
	defer close(done)

	for {
		incomingFrame, err := readFrame(state.conn)
		if err != nil {
			return err
		}

		logFrame("RECEIVED FRAME", incomingFrame)

		switch incomingFrame.Type {
		case msgTypePingRequest:
			fmt.Printf("[%s] received PING tid=%d, sending PONG\n", ts(), incomingFrame.TID)
			if err := writeFrame(state, &frame{
				Type: msgTypePingResponse,
				TID:  incomingFrame.TID,
			}, "SENDING PONG"); err != nil {
				return err
			}
		case msgTypePingResponse:
			fmt.Printf("[%s] received PONG tid=%d\n", ts(), incomingFrame.TID)
		case msgTypeHelloResponse:
			fmt.Printf("[%s] received unexpected HELLO response after handshake\n", ts())
		default:
			if isRequestType(incomingFrame.Type) {
				if err := handleIncomingRequest(state, incomingFrame); err != nil {
					fmt.Printf("[%s] failed to handle request tid=%d: %v\n", ts(), incomingFrame.TID, err)
				}
				continue
			}

			if isResponseType(incomingFrame.Type) {
				logResponse(incomingFrame)
				continue
			}

			fmt.Printf("[%s] unknown frame type=0x%02X\n", ts(), incomingFrame.Type)
		}
	}
}

func handleIncomingRequest(state *connectionState, reqFrame *frame) error {
	var req requestPayload
	if len(reqFrame.Payload) > 0 {
		if err := json.Unmarshal(reqFrame.Payload, &req); err != nil {
			return fmt.Errorf("parse request payload: %w", err)
		}
	}

	fmt.Printf("[%s] incoming REQUEST tid=%d request_id=%d action=%s\n",
		ts(), reqFrame.TID, req.RequestID, req.Action)

	respPayload, err := json.Marshal(responsePayload{
		RequestID: req.RequestID,
		Status:    "success",
		Data: map[string]interface{}{
			"echo_action":     req.Action,
			"echo_request_id": req.RequestID,
			"source_type":     fmt.Sprintf("0x%02X", reqFrame.Type),
			"handled_at":      time.Now().Format(time.RFC3339),
		},
		Timestamp:   time.Now().Unix(),
		ProcessedBy: "tcp-simulator-server",
	})
	if err != nil {
		return fmt.Errorf("marshal response payload: %w", err)
	}

	return writeFrame(state, &frame{
		Type:    reqFrame.Type + 1,
		Length:  uint16(len(respPayload)),
		TID:     reqFrame.TID,
		Payload: respPayload,
	}, "SENDING RESPONSE")
}

func logResponse(respFrame *frame) {
	var resp responsePayload
	if len(respFrame.Payload) == 0 {
		fmt.Printf("[%s] response tid=%d has empty payload\n", ts(), respFrame.TID)
		return
	}

	if err := json.Unmarshal(respFrame.Payload, &resp); err != nil {
		fmt.Printf("[%s] response tid=%d payload=%s\n", ts(), respFrame.TID, string(respFrame.Payload))
		return
	}

	fmt.Printf("[%s] received RESPONSE tid=%d request_id=%d status=%s payload=%s\n",
		ts(), respFrame.TID, resp.RequestID, resp.Status, string(respFrame.Payload))
}

func readFrame(conn net.Conn) (*frame, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint16(header[2:4])
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			return nil, err
		}
	}

	return &frame{
		Type:    header[1],
		Length:  length,
		TID:     binary.BigEndian.Uint32(header[4:8]),
		Payload: payload,
	}, nil
}

func writeFrame(state *connectionState, frm *frame, label string) error {
	header := make([]byte, 8)
	header[0] = 0x00
	header[1] = frm.Type
	binary.BigEndian.PutUint16(header[2:4], uint16(len(frm.Payload)))
	binary.BigEndian.PutUint32(header[4:8], frm.TID)
	frm.Length = uint16(len(frm.Payload))

	state.writeMu.Lock()
	defer state.writeMu.Unlock()

	logFrame(label, frm)
	_, err := state.conn.Write(append(header, frm.Payload...))
	return err
}

func logFrame(label string, frm *frame) {
	header := make([]byte, 8)
	header[0] = 0x00
	header[1] = frm.Type
	binary.BigEndian.PutUint16(header[2:4], frm.Length)
	binary.BigEndian.PutUint32(header[4:8], frm.TID)

	fmt.Printf("\n[%s] ========== %s ==========\n", ts(), label)
	fmt.Printf("header: %02X %02X %02X %02X %02X %02X %02X %02X\n",
		header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7])
	fmt.Printf("msg_type=0x%02X length=%d tid=%d\n", frm.Type, frm.Length, frm.TID)
	if len(frm.Payload) > 0 {
		fmt.Printf("body: %s\n", string(frm.Payload))
	}
	fmt.Printf("====================================\n")
}

func isRequestType(msgType uint8) bool {
	return msgType >= 0x05 && msgType%2 == 1
}

func isResponseType(msgType uint8) bool {
	return msgType >= 0x06 && msgType%2 == 0
}

func parseHexByte(value string) (uint8, error) {
	var parsed uint8
	_, err := fmt.Sscanf(value, "%x", &parsed)
	return parsed, err
}
