package worker

import (
	"fmt"
	"sync/atomic"
	"time"

	"tcp-bridge/internal/config"

	"github.com/nats-io/nats.go"
)

var (
	globalTIDCounter uint32
	lastResetDate    int32
)

// resolveOutboundRoute resolves outbound request and response types by subject.
func (h *BridgeHandler) resolveOutboundRoute(subject string) (uint8, uint8, bool) {
	route, exists := h.natsConfig.MessageTypeRouting.GetOutboundRouteBySubject(subject)
	if !exists {
		return 0, 0, false
	}

	requestType := uint8(0)
	for msgType, candidate := range h.natsConfig.MessageTypeRouting.Outbound {
		if candidate.Subject != subject {
			continue
		}
		fmt.Sscanf(msgType, "%02x", &requestType)
		break
	}

	var responseType uint8
	fmt.Sscanf(route.ResponseMsgType, "%02x", &responseType)

	if requestType == 0 {
		return 0, 0, false
	}

	return requestType, responseType, true
}

func (h *BridgeHandler) sendTCPResponse(tid uint32, msgType uint8, payload []byte) {
	responseFrame := &config.Frame{
		Type:    msgType,
		TID:     tid,
		Payload: payload,
	}

	if err := h.tcpSender.SendFrame(responseFrame); err != nil {
		h.logger.Error("failed to send TCP response", "tid", tid, "msg_type", fmt.Sprintf("0x%02x", msgType), "error", err)
	} else {
		h.logger.Debug("sent TCP response", "tid", tid, "msg_type", fmt.Sprintf("0x%02x", msgType))
	}
}

func (h *BridgeHandler) sendTCPResponseToConnection(connectionID string, tid uint32, msgType uint8, payload []byte) {
	responseFrame := &config.Frame{
		Type:    msgType,
		TID:     tid,
		Payload: payload,
	}

	if err := h.tcpSender.SendFrameToConnection(connectionID, responseFrame); err != nil {
		h.logger.Error("failed to send TCP response to connection",
			"connection_id", connectionID,
			"tid", tid,
			"msg_type", fmt.Sprintf("0x%02x", msgType),
			"error", err)
	} else {
		h.logger.Debug("sent TCP response to connection",
			"connection_id", connectionID,
			"tid", tid,
			"msg_type", fmt.Sprintf("0x%02x", msgType))
	}
}

func (h *BridgeHandler) sendTCPErrorResponse(tid uint32, msgType uint8, errMsg string) {
	errorPayload := []byte(fmt.Sprintf(`{"error":"%s"}`, errMsg))
	h.sendTCPResponse(tid, msgType, errorPayload)
}

func (h *BridgeHandler) sendTCPErrorResponseToConnection(connectionID string, tid uint32, msgType uint8, errMsg string) {
	errorPayload := []byte(fmt.Sprintf(`{"error":"%s"}`, errMsg))
	h.sendTCPResponseToConnection(connectionID, tid, msgType, errorPayload)
}

func (h *BridgeHandler) replyError(msg *nats.Msg, errMsg string) {
	if msg.Reply != "" {
		errorResponse := []byte(fmt.Sprintf(`{"error":"%s"}`, errMsg))
		replyPublisher := h.natsClient.GetReplyPublisher()
		if err := replyPublisher.PublishReply(msg.Reply, errorResponse); err != nil {
			h.logger.Error("failed to send error reply", "error", err)
		}
	}
}

// generateTID generates a sequential transaction ID with daily reset.
func generateTID() uint32 {
	now := time.Now()
	currentDate := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	lastDate := atomic.LoadInt32(&lastResetDate)
	if currentDate != lastDate {
		if atomic.CompareAndSwapInt32(&lastResetDate, lastDate, currentDate) {
			atomic.StoreUint32(&globalTIDCounter, 0)
		}
	}

	tid := atomic.AddUint32(&globalTIDCounter, 1)
	if tid == 0 {
		atomic.StoreUint32(&globalTIDCounter, 1)
		return 1
	}

	return tid
}
