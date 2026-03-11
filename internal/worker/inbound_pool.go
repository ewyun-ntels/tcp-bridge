package worker

import (
	"fmt"
	"log/slog"
	"time"

	"tcp-bridge/internal/config"
	"tcp-bridge/internal/inflight"
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
	logger := p.logger.With("tid", frame.TID, "msg_type", fmt.Sprintf("0x%02x", frame.Type))
	logger.Debug("processing TCP request")

	if !p.handler.natsConfig.MessageTypeRouting.IsInboundRequestType(frame.Type) {
		logger.Warn("received non-TCP->NATS request type, processing anyway", "type", frame.Type)
	}

	subject := p.handler.natsConfig.MessageTypeRouting.GetInboundSubjectForMessageType(frame.Type)
	if subject == "" {
		logger.Error("unknown message type, dropping",
			"msg_type", fmt.Sprintf("0x%02x", frame.Type),
			"connection_id", frame.ConnectionID)
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
		"request_type", fmt.Sprintf("0x%02x", frame.Type),
		"response_type", fmt.Sprintf("0x%02x", responseType))

	tcpReplyInfo := &inflight.TCPReplyInfo{
		ConnectionID: frame.ConnectionID,
		FrameType:    responseType,
	}
	p.handler.inflightMgr.GetInflightB().Register(frame.TID, tcpReplyInfo, frame.Payload, deadline)

	requester := p.handler.natsClient.GetRequester()
	response, err := requester.RequestWithTimeoutToSubject(subject, frame.Payload, p.config.Timeout)
	if err != nil {
		logger.Error("NATS request failed", "subject", subject, "error", err)
		p.handler.inflightMgr.GetInflightB().Remove(frame.ConnectionID, frame.TID)
		p.handler.sendTCPErrorResponseToConnection(frame.ConnectionID, frame.TID, responseType, "internal error")
		return
	}

	logger.Debug("received NATS response", "subject", subject, "response_size", len(response))
	p.handler.sendTCPResponseToConnection(frame.ConnectionID, frame.TID, responseType, response)
	p.handler.inflightMgr.GetInflightB().Remove(frame.ConnectionID, frame.TID)
}
