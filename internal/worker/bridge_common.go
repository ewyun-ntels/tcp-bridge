package worker

import (
	"encoding/json"
	"fmt"
	"strings"

	"tcp-bridge/internal/config"

	"github.com/nats-io/nats.go"
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

func (h *BridgeHandler) sendTCPResponseToConnection(connectionID string, tid uint32, msgType uint8, payload []byte) error {
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
		return err
	}

	h.logger.Debug("sent TCP response to connection",
		"connection_id", connectionID,
		"tid", tid,
		"msg_type", fmt.Sprintf("0x%02x", msgType))
	return nil
}

func (h *BridgeHandler) sendTCPErrorResponseToConnection(connectionID string, tid uint32, msgType uint8) error {
	errorPayload, err := h.buildTCPErrorPayload()
	if err != nil {
		return err
	}
	return h.sendTCPResponseToConnection(connectionID, tid, msgType, errorPayload)
}

func (h *BridgeHandler) buildTCPErrorPayload() ([]byte, error) {
	if h.pgFormatter != nil && h.pgFormatter.Template.PayloadTemplate != nil {
		payload, err := json.Marshal(h.pgFormatter.Template.PayloadTemplate)
		if err != nil {
			return nil, fmt.Errorf("marshal pg response formatter payload template: %w", err)
		}
		return payload, nil
	}

	errorResp := map[string]string{"error": "internal error"}
	errorPayload, err := json.Marshal(errorResp)
	if err != nil {
		return nil, fmt.Errorf("marshal fallback error payload: %w", err)
	}
	return errorPayload, nil
}

func (h *BridgeHandler) replyError(msg *nats.Msg, errMsg string) error {
	if msg.Reply == "" {
		return fmt.Errorf("reply subject is empty")
	}

	replyPublisher := h.natsClient.GetReplyPublisher()
	if err := replyPublisher.PublishError(msg.Reply, h.outboundConfig.ErrorReply.ResultCode, errMsg); err != nil {
		h.logger.Error("failed to send error reply", "error", err)
		return err
	}
	return nil
}

func formatMsgType(msgType uint8) string {
	return fmt.Sprintf("0x%02x", msgType)
}

func classifyInboundWriteError(err error) string {
	if err == nil {
		return ""
	}
	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "is not ready"):
		return "connection_not_ready"
	default:
		return "tcp_write_error"
	}
}
