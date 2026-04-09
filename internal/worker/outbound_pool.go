package worker

import (
	"log/slog"
	"strings"
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
	jobs    chan *outboundJob
}

type outboundJob struct {
	msg        *nats.Msg
	receivedAt time.Time
	enqueuedAt time.Time
}

func (p *OutboundWorkerPool) Start() {
	p.jobs = make(chan *outboundJob, p.config.QueueSize)
	p.startWorkers(p.config.WorkerCount)
}

func (p *OutboundWorkerPool) Enqueue(msg *nats.Msg) bool {
	job := &outboundJob{
		msg:        msg,
		receivedAt: time.Now(),
		enqueuedAt: time.Now(),
	}
	select {
	case <-p.handler.ctx.Done():
		return false
	case p.jobs <- job:
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
		case job := <-p.jobs:
			if job != nil {
				p.process(job)
			}
		}
	}
}

func (p *OutboundWorkerPool) process(job *outboundJob) {
	msg := job.msg
	logger := p.logger.With("subject", msg.Subject, "reply", msg.Reply)
	logger.Debug("processing external request")
	metricsConnectionID := ""
	finalStatus := "error"
	subject := msg.Subject
	msgTypeLabel := "unknown"
	defer func() {
		if !job.receivedAt.IsZero() {
			p.handler.metrics.ObserveOutboundEndToEndDuration(metricsConnectionID, subject, msgTypeLabel, finalStatus, time.Since(job.receivedAt))
		}
	}()

	if msg.Reply == "" {
		logger.Error(
			"rejecting outbound message without reply subject",
			"subject", msg.Subject,
			"size", len(msg.Data))
		return
	}

	tid := tid.Next()
	msgType, expectedRespType, ok := p.handler.resolveOutboundRoute(msg.Subject)
	if !ok {
		logger.Error("unknown NATS subject for NATS->TCP flow", "subject", msg.Subject)
		p.handler.metrics.IncOutboundRequests(metricsConnectionID, subject, msgTypeLabel)
		p.handler.metrics.IncOutboundResponses(metricsConnectionID, subject, msgTypeLabel, "dropped")
		finalStatus = "dropped"
		p.handler.replyError(msg, "unknown NATS subject")
		return
	}
	msgTypeLabel = formatMsgType(msgType)
	p.handler.metrics.IncOutboundRequests(metricsConnectionID, subject, msgTypeLabel)

	frame := &config.Frame{
		Type:    msgType,
		TID:     tid,
		Payload: msg.Data,
	}

	retryAttempts := p.config.RetryAttempts
	responseTimeout := p.config.ResponseTimeout
	priorityCount := len(p.handler.connMgr.GetConnections())
	totalTimeout := time.Duration(retryAttempts*priorityCount)*responseTimeout + 10*time.Second
	deadline := time.Now().Add(totalTimeout).Unix()

	inflightEntry := p.handler.inflightMgr.GetInflightA().Register(
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
		"msg_type", msgTypeLabel,
		"expected_response_type", formatMsgType(expectedRespType),
		"deadline_seconds", totalTimeout.Seconds())

	success, status, selectedConnectionID := p.sendAndWaitWithRetry(frame, inflightEntry, logger)
	if selectedConnectionID != "" {
		metricsConnectionID = selectedConnectionID
	}
	if !success {
		logger.Error("all connection attempts failed", "tid", tid)
		p.handler.inflightMgr.GetInflightA().Remove(tid)
		p.handler.metrics.IncOutboundResponses(metricsConnectionID, subject, msgTypeLabel, status)
		finalStatus = status
		p.handler.replyError(msg, "all TCP connections failed or timeout")
		return
	}

	p.handler.metrics.IncOutboundResponses(metricsConnectionID, subject, msgTypeLabel, "success")
	finalStatus = "success"
	logger.Debug("request processed successfully", "tid", tid)
}

func (p *OutboundWorkerPool) sendAndWaitWithRetry(
	frame *config.Frame,
	inflightEntry *inflight.InflightEntryA,
	logger *slog.Logger,
) (bool, string, string) {
	connections := p.handler.connMgr.GetConnections()
	retryAttempts := p.config.RetryAttempts
	responseTimeout := p.config.ResponseTimeout
	startedAt := time.Now()
	msgTypeLabel := formatMsgType(frame.Type)
	lastConnectionID := ""
	timedOut := false
	readyConnectionSeen := false

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
		readyConnectionSeen = true

		for attempt := 1; attempt <= retryAttempts; attempt++ {
			lastConnectionID = conn.ID()
			logger.Info("attempting TCP send",
				"tid", frame.TID,
				"msg_type", msgTypeLabel,
				"connection_id", conn.ID(),
				"priority", priority,
				"attempt", attempt,
				"payload_size", len(frame.Payload),
				"elapsed_ms", time.Since(startedAt).Milliseconds())

			data, err := frame.Serialize()
			if err != nil {
				logger.Error("failed to serialize frame", "tid", frame.TID, "error", err)
				return false, "error", lastConnectionID
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

			select {
			case responseFrame := <-inflightEntry.ResponseChan:
				logger.Info("received TCP response",
					"tid", frame.TID,
					"connection_id", responseFrame.ConnectionID,
					"priority", priority,
					"attempt", attempt,
					"response_type", formatMsgType(responseFrame.Type),
					"response_size", len(responseFrame.Payload))

				replyPublisher := p.handler.natsClient.GetReplyPublisher()
				if err := replyPublisher.PublishReply(inflightEntry.ReplySubject, responseFrame.Payload); err != nil {
					logger.Error("failed to send NATS reply", "tid", frame.TID, "error", err)
					return false, "error", responseFrame.ConnectionID
				}

				logger.Debug("sent NATS reply (payload only)", "tid", frame.TID, "size", len(responseFrame.Payload))
				return true, "success", responseFrame.ConnectionID

			case <-time.After(responseTimeout):
				timedOut = true
				logger.Warn("response timeout, retrying",
					"tid", frame.TID,
					"msg_type", msgTypeLabel,
					"connection_id", conn.ID(),
					"priority", priority,
					"attempt", attempt,
					"response_timeout", responseTimeout,
					"elapsed_ms", time.Since(startedAt).Milliseconds())
				continue
			}
		}

		nextPriority := -1
		for i := priority + 1; i < len(connections); i++ {
			if connections[i] != nil {
				nextPriority = i
				break
			}
		}
		logger.Warn("all attempts failed for priority, trying next",
			"tid", frame.TID,
			"failed_priority", priority,
			"connection_id", conn.ID(),
			"attempts", retryAttempts,
			"next_priority", nextPriority,
			"will_retry_on_other_connection", nextPriority >= 0,
			"elapsed_ms", time.Since(startedAt).Milliseconds())
	}

	if timedOut {
		return false, "timeout", lastConnectionID
	}
	if !readyConnectionSeen {
		return false, "error", lastConnectionID
	}
	return false, "error", lastConnectionID
}

func classifyOutboundWriteError(err error) string {
	if err == nil {
		return ""
	}
	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "no ready connection available"):
		return "no_ready_connection"
	default:
		return "tcp_write_error"
	}
}
