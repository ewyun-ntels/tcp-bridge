package worker

import (
	"fmt"
	"log/slog"
	"time"

	"tcp-bridge/internal/config"
	"tcp-bridge/internal/inflight"
	"tcp-bridge/internal/tid"

	"github.com/nats-io/nats.go"
)

// OutboundWorkerPool owns the outbound queue and workers.
type OutboundWorkerPool struct {
	handler *BridgeHandler
	logger  *slog.Logger
	config  *config.MessageHandlerPoolConfig
	jobs    chan *nats.Msg
}

func (p *OutboundWorkerPool) Start() {
	p.jobs = make(chan *nats.Msg, p.config.QueueSize)
	p.startWorkers(p.config.WorkerCount)
}

func (p *OutboundWorkerPool) Enqueue(msg *nats.Msg) bool {
	select {
	case <-p.handler.ctx.Done():
		return false
	case p.jobs <- msg:
		return true
	default:
		p.logger.Warn("queue is full", "queue_size", cap(p.jobs))
		return false
	}
}

func (p *OutboundWorkerPool) startWorkers(count int) {
	for i := 0; i < count; i++ {
		workerID := i
		go p.workerLoop(workerID)
	}
}

func (p *OutboundWorkerPool) workerLoop(workerID int) {
	logger := p.logger.With("worker_id", workerID)
	logger.Info("worker started")
	defer logger.Info("worker stopped")

	for {
		select {
		case <-p.handler.ctx.Done():
			return
		case msg := <-p.jobs:
			if msg != nil {
				p.process(msg)
			}
		}
	}
}

func (p *OutboundWorkerPool) process(msg *nats.Msg) {
	logger := p.logger.With("subject", msg.Subject, "reply", msg.Reply)
	logger.Debug("processing external request")

	tid := tid.Next()
	isRPC := msg.Reply != ""
	msgType, expectedRespType, ok := p.handler.resolveOutboundRoute(msg.Subject)
	if !ok {
		logger.Error("unknown NATS subject for NATS->TCP flow", "subject", msg.Subject)
		p.handler.replyError(msg, "unknown NATS subject")
		return
	}

	frame := &config.Frame{
		Type:    msgType,
		TID:     tid,
		Payload: msg.Data,
	}

	var inflightEntry *inflight.InflightEntryA
	if isRPC {
		retryAttempts := p.config.RetryAttempts
		responseTimeout := p.config.ResponseTimeout
		priorityCount := len(p.handler.connMgr.GetConnections())
		totalTimeout := time.Duration(retryAttempts*priorityCount)*responseTimeout + 10*time.Second
		deadline := time.Now().Add(totalTimeout).Unix()

		inflightEntry = p.handler.inflightMgr.GetInflightA().Register(
			tid,
			msg.Reply,
			msg.Subject,
			msgType,
			expectedRespType,
			deadline,
		)
		logger.Debug("registered inflight-A entry",
			"tid", tid,
			"subject", msg.Subject,
			"msg_type", fmt.Sprintf("0x%02x", msgType),
			"expected_response_type", fmt.Sprintf("0x%02x", expectedRespType),
			"deadline_seconds", totalTimeout.Seconds())
	}

	success := p.sendAndWaitWithRetry(frame, inflightEntry, isRPC, logger)
	if !success {
		logger.Error("all connection attempts failed", "tid", tid)
		if isRPC {
			p.handler.inflightMgr.GetInflightA().Remove(tid)
			p.handler.replyError(msg, "all TCP connections failed or timeout")
		}
		return
	}

	logger.Debug("request processed successfully", "tid", tid)
}

func (p *OutboundWorkerPool) sendAndWaitWithRetry(
	frame *config.Frame,
	inflightEntry *inflight.InflightEntryA,
	isRPC bool,
	logger *slog.Logger,
) bool {
	connections := p.handler.connMgr.GetConnections()
	retryAttempts := p.config.RetryAttempts
	responseTimeout := p.config.ResponseTimeout

	for priority := 0; priority < len(connections); priority++ {
		conn := connections[priority]
		if conn == nil {
			continue
		}

		if conn.GetState() != config.ConnStateReady {
			logger.Debug("connection not ready, skipping to next priority",
				"tid", frame.TID,
				"priority", priority,
				"state", conn.GetState())
			continue
		}

		for attempt := 1; attempt <= retryAttempts; attempt++ {
			data, err := frame.Serialize()
			if err != nil {
				logger.Error("failed to serialize frame", "tid", frame.TID, "error", err)
				return false
			}

			bytesWritten, err := conn.Write(data)
			if err != nil {
				logger.Warn("TCP send failed",
					"tid", frame.TID,
					"priority", priority,
					"attempt", attempt,
					"error", err)
				continue
			}

			if bytesWritten != len(data) {
				logger.Error("incomplete write",
					"tid", frame.TID,
					"priority", priority,
					"wrote", bytesWritten,
					"expected", len(data))
				continue
			}

			logger.Info("TCP frame sent successfully",
				"tid", frame.TID,
				"connection_id", conn.ID(),
				"priority", priority,
				"attempt", attempt,
				"bytes", bytesWritten)

			if inflightEntry != nil {
				inflightEntry.ConnectionID = conn.ID()
				p.handler.inflightMgr.GetInflightA().SetConnectionID(frame.TID, conn.ID())
			}

			if !isRPC {
				return true
			}

			select {
			case responseFrame := <-inflightEntry.ResponseChan:
				logger.Info("received TCP response",
					"tid", frame.TID,
					"connection_id", responseFrame.ConnectionID,
					"priority", priority,
					"attempt", attempt,
					"response_type", fmt.Sprintf("0x%02x", responseFrame.Type),
					"response_size", len(responseFrame.Payload))

				replyPublisher := p.handler.natsClient.GetReplyPublisher()
				if err := replyPublisher.PublishReply(inflightEntry.ReplySubject, responseFrame.Payload); err != nil {
					logger.Error("failed to send NATS reply", "tid", frame.TID, "error", err)
					return false
				}

				logger.Debug("sent NATS reply (payload only)", "tid", frame.TID, "size", len(responseFrame.Payload))
				return true

			case <-time.After(responseTimeout):
				logger.Warn("response timeout, retrying",
					"tid", frame.TID,
					"priority", priority,
					"attempt", attempt,
					"response_timeout", responseTimeout)
				continue
			}
		}

		logger.Warn("all attempts failed for priority, trying next",
			"tid", frame.TID,
			"failed_priority", priority,
			"attempts", retryAttempts)
	}

	return false
}
