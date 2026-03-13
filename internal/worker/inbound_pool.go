package worker

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"tcp-bridge/internal/config"
	"tcp-bridge/internal/inflight"

	"github.com/nats-io/nats.go"
)

// InboundWorkerPool owns the inbound queue and workers.
type InboundWorkerPool struct {
	handler *BridgeHandler
	logger  *slog.Logger
	config  *config.MessageHandlerPoolConfig
	jobs    chan *config.Frame
}

func (p *InboundWorkerPool) Start() {
	p.jobs = make(chan *config.Frame, p.config.QueueSize)
	p.startWorkers(p.config.WorkerCount)
}

func (p *InboundWorkerPool) Enqueue(frame *config.Frame) bool {
	select {
	case <-p.handler.ctx.Done():
		return false
	case p.jobs <- frame:
		return true
	default:
		p.logger.Warn("queue is full", "queue_size", cap(p.jobs))
		return false
	}
}

func (p *InboundWorkerPool) startWorkers(count int) {
	for i := 0; i < count; i++ {
		workerID := i
		go p.workerLoop(workerID)
	}
}

func (p *InboundWorkerPool) workerLoop(workerID int) {
	logger := p.logger.With("worker_id", workerID)
	logger.Info("worker started")
	defer logger.Info("worker stopped")

	for {
		select {
		case <-p.handler.ctx.Done():
			return
		case frame := <-p.jobs:
			if frame != nil {
				p.process(frame)
			}
		}
	}
}

func (p *InboundWorkerPool) process(frame *config.Frame) {
	msgType := formatMsgType(frame.Type)
	logger := p.logger.With("tid", frame.TID, "msg_type", msgType)
	logger.Debug("processing TCP request")
	startedAt := time.Now()
	finalStatus := "error"
	defer func() {
		p.handler.metrics.IncInboundWorkerTasks(finalStatus)
		p.handler.metrics.AddInboundInflight(msgType, -1)
		if !frame.ReceivedAt.IsZero() {
			p.handler.metrics.ObserveInboundEndToEndDuration(msgType, finalStatus, time.Since(frame.ReceivedAt))
		}
	}()

	if !frame.EnqueuedAt.IsZero() {
		p.handler.metrics.ObserveInboundQueueWaitDuration(time.Since(frame.EnqueuedAt))
	}

	if !p.handler.natsConfig.MessageTypeRouting.IsInboundRequestType(frame.Type) {
		logger.Warn("received non-TCP->NATS request type, processing anyway", "type", frame.Type)
	}

	subject := p.handler.natsConfig.MessageTypeRouting.GetInboundSubjectForMessageType(frame.Type)
	if subject == "" {
		logger.Error("unknown message type, dropping",
			"msg_type", msgType,
			"connection_id", frame.ConnectionID)
		p.handler.metrics.IncInboundErrors(msgType, "invalid_message_type")
		p.handler.metrics.IncInboundResponses(msgType, "dropped")
		finalStatus = "dropped"
		return
	}
	logger.Debug("routing to NATS", "subject", subject)

	deadline := time.Now().Add(p.config.Timeout).Unix()

	responseType := p.handler.natsConfig.MessageTypeRouting.GetResponseType(frame.Type)
	if responseType == 0 {
		responseType = frame.Type
		logger.Warn("response type not configured, using auto-calculated type",
			"request_type", fmt.Sprintf("0x%02x", frame.Type),
			"response_type", fmt.Sprintf("0x%02x", responseType))
	}

	logger.Debug("determined response type",
		"request_type", msgType,
		"response_type", formatMsgType(responseType))

	tcpReplyInfo := &inflight.TCPReplyInfo{
		ConnectionID: frame.ConnectionID,
		FrameType:    responseType,
	}
	p.handler.inflightMgr.GetInflightB().Register(frame.TID, frame.Type, tcpReplyInfo, frame.Payload, deadline)

	requester := p.handler.natsClient.GetRequester()
	natsStartedAt := time.Now()
	response, err := requester.RequestWithTimeoutToSubject(subject, frame.Payload, p.config.Timeout)
	if err != nil {
		logger.Error("NATS request failed", "subject", subject, "error", err)
		p.handler.inflightMgr.GetInflightB().Remove(frame.ConnectionID, frame.TID)
		if errors.Is(err, nats.ErrTimeout) {
			finalStatus = "timeout"
			p.handler.metrics.IncInboundErrors(msgType, "nats_timeout")
			p.handler.metrics.IncInboundTimeout(msgType, "nats_wait")
			p.handler.metrics.IncInboundTimeout(msgType, "overall")
			p.handler.metrics.ObserveInboundNATSRequestDuration(msgType, "timeout", time.Since(natsStartedAt))
		} else {
			finalStatus = "error"
			p.handler.metrics.IncInboundErrors(msgType, "nats_error")
			p.handler.metrics.ObserveInboundNATSRequestDuration(msgType, "error", time.Since(natsStartedAt))
		}

		writeStartedAt := time.Now()
		writeErr := p.handler.sendTCPErrorResponseToConnection(frame.ConnectionID, frame.TID, responseType, "internal error")
		p.handler.metrics.ObserveInboundResponseWriteDuration(msgType, finalStatus, time.Since(writeStartedAt))
		if writeErr != nil {
			p.handler.metrics.IncInboundErrors(msgType, classifyInboundWriteError(writeErr))
		}
		p.handler.metrics.IncInboundResponses(msgType, finalStatus)
		return
	}

	p.handler.metrics.ObserveInboundNATSRequestDuration(msgType, "success", time.Since(natsStartedAt))
	logger.Debug("received NATS response", "subject", subject, "response_size", len(response))
	writeStartedAt := time.Now()
	if err := p.handler.sendTCPResponseToConnection(frame.ConnectionID, frame.TID, responseType, response); err != nil {
		logger.Error("failed to send TCP response", "error", err, "elapsed_ms", time.Since(startedAt).Milliseconds())
		finalStatus = "error"
		p.handler.metrics.ObserveInboundResponseWriteDuration(msgType, "error", time.Since(writeStartedAt))
		p.handler.metrics.IncInboundErrors(msgType, classifyInboundWriteError(err))
		p.handler.metrics.IncInboundResponses(msgType, "error")
		p.handler.inflightMgr.GetInflightB().Remove(frame.ConnectionID, frame.TID)
		return
	}
	p.handler.metrics.ObserveInboundResponseWriteDuration(msgType, "success", time.Since(writeStartedAt))
	p.handler.metrics.IncInboundResponses(msgType, "success")
	finalStatus = "success"
	p.handler.inflightMgr.GetInflightB().Remove(frame.ConnectionID, frame.TID)
}
