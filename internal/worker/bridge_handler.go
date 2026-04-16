package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"tcp-bridge/internal/config"
	"tcp-bridge/internal/connection"
	"tcp-bridge/internal/inflight"
	"tcp-bridge/internal/metrics"
	tcpnats "tcp-bridge/internal/nats"
	"tcp-bridge/internal/tcp"

	"github.com/nats-io/nats.go"
)

// BridgeHandler owns the directional worker pools and shared dependencies.
type BridgeHandler struct {
	logger         *slog.Logger
	outboundConfig *config.MessageHandlerPoolConfig
	inboundConfig  *config.MessageHandlerPoolConfig
	natsConfig     *config.NATSConfig

	natsClient  *tcpnats.Client
	tcpSender   *tcp.Sender
	inflightMgr *inflight.InflightManager
	connMgr     *connection.ConnectionManager
	metrics     *metrics.Metrics

	ctx    context.Context
	cancel context.CancelFunc

	outboundPool *OutboundWorkerPool
	inboundPool  *InboundWorkerPool
}

// NewBridgeHandler creates a new bridge handler.
func NewBridgeHandler(
	logger *slog.Logger,
	outboundConfig *config.MessageHandlerPoolConfig,
	inboundConfig *config.MessageHandlerPoolConfig,
	natsConfig *config.NATSConfig,
	natsClient *tcpnats.Client,
	tcpSender *tcp.Sender,
	inflightMgr *inflight.InflightManager,
	connMgr *connection.ConnectionManager,
	metrics *metrics.Metrics,
) *BridgeHandler {
	handler := &BridgeHandler{
		logger:         logger,
		outboundConfig: outboundConfig,
		inboundConfig:  inboundConfig,
		natsConfig:     natsConfig,
		natsClient:     natsClient,
		tcpSender:      tcpSender,
		inflightMgr:    inflightMgr,
		connMgr:        connMgr,
		metrics:        metrics,
	}

	handler.outboundPool = &OutboundWorkerPool{
		handler: handler,
		logger:  logger.With("worker_pool", "outbound"),
		config:  outboundConfig,
	}
	handler.inboundPool = &InboundWorkerPool{
		handler: handler,
		logger:  logger.With("worker_pool", "inbound"),
		config:  inboundConfig,
	}
	handler.inflightMgr.GetInflightB().SetExpireCallback(handler.handleInboundInflightExpiry)

	return handler
}

// Start starts the NATS subscription and both worker pools.
func (h *BridgeHandler) Start(ctx context.Context) error {
	h.logger.Info("starting bridge handler with separated worker pools")

	if ctx == nil {
		ctx = context.Background()
	}
	h.ctx, h.cancel = context.WithCancel(ctx)

	h.outboundPool.Start()
	h.inboundPool.Start()

	subscriber := h.natsClient.GetSubscriber()
	subjects := h.natsConfig.MessageTypeRouting.GetOutboundSubjects()
	if err := subscriber.SubscribeSubjectsWithQueue(subjects, "tcp-bridge-workers", h.HandleOutboundRequest); err != nil {
		return fmt.Errorf("failed to subscribe to external requests: %w", err)
	}

	h.logger.Info("bridge handler started with separated worker pools")
	return nil
}

// Stop stops the bridge handler.
func (h *BridgeHandler) Stop() {
	h.logger.Info("stopping bridge handler")

	if h.cancel != nil {
		h.cancel()
	}

	subscriber := h.natsClient.GetSubscriber()
	if err := subscriber.Unsubscribe(); err != nil {
		h.logger.Error("error unsubscribing from NATS", "error", err)
	}

	h.logger.Info("bridge handler stopped")
}

// HandleOutboundRequest enqueues outbound work into the dedicated pool.
func (h *BridgeHandler) HandleOutboundRequest(msg *nats.Msg) {
	metricsConnectionID := ""
	msgTypeLabel := "unknown"
	if msgType, _, ok := h.resolveOutboundRoute(msg.Subject); ok {
		msgTypeLabel = formatMsgType(msgType)
	}
	h.logger.Info("outbound NATS request received",
		"direction", "outbound",
		"stage", "nats_request_received",
		"subject", msg.Subject,
		"reply", msg.Reply,
		"msg_type", msgTypeLabel,
		"payload_size", len(msg.Data))

	if !h.outboundPool.Enqueue(msg) {
		h.metrics.IncOutboundRequests(metricsConnectionID, msg.Subject, msgTypeLabel)
		if err := h.replyError(msg, "outbound worker queue is full"); err == nil {
			h.metrics.IncOutboundResponsesSent(metricsConnectionID, msg.Subject, msgTypeLabel, responseSendResultError)
		}
		h.metrics.IncOutboundOutcomes(metricsConnectionID, msg.Subject, msgTypeLabel, outboundStatusDropped, outboundReasonQueueFull)
	}
}

// HandleInboundFrame enqueues inbound work into the dedicated pool.
func (h *BridgeHandler) HandleInboundFrame(frame *config.Frame) {
	frame.EnqueuedAt = time.Now()
	msgType := formatMsgType(frame.Type)

	h.logger.Info("inbound TCP request received",
		"stage", "tcp_request_received before_enqueue",
		"connection_id", frame.ConnectionID,
		"tid", frame.TID,
		"msg_type", msgType,
		"payload_size", len(frame.Payload))

	if !h.inboundPool.Enqueue(frame) {
		logger := h.logger.With("tid", frame.TID, "connection_id", frame.ConnectionID, "msg_type", msgType)
		logger.Error("inbound worker queue is full")

		responseType := h.natsConfig.MessageTypeRouting.GetResponseType(frame.Type)
		if responseType == 0 {
			responseType = frame.Type
		}
		if err := h.sendTCPErrorResponseToConnection(frame.ConnectionID, frame.TID, responseType, "inbound worker queue is full"); err != nil {
			logger.Error("failed to send TCP error response for dropped frame", "error", err)
		} else {
			h.metrics.IncInboundResponsesSent(frame.ConnectionID, msgType, responseSendResultError)
		}
		h.metrics.IncInboundOutcomes(frame.ConnectionID, msgType, inboundStatusDropped, inboundReasonQueueFull)
	}
}

// FrameDispatcher dispatches inbound TCP request frames to the bridge handler.
type FrameDispatcher struct {
	logger *slog.Logger
	bridge *BridgeHandler
}

// NewFrameDispatcher creates a new frame dispatcher.
func NewFrameDispatcher(logger *slog.Logger, bridge *BridgeHandler) *FrameDispatcher {
	return &FrameDispatcher{
		logger: logger,
		bridge: bridge,
	}
}

// DispatchRequestFrame dispatches a TCP request frame directly to the inbound worker pool.
func (d *FrameDispatcher) DispatchRequestFrame(frame *config.Frame) {
	if !d.bridge.natsConfig.MessageTypeRouting.IsInboundRequestType(frame.Type) {
		d.logger.Warn("non-inbound request frame passed to handler",
			"type", fmt.Sprintf("0x%02x", frame.Type), "tid", frame.TID)
		return
	}

	d.bridge.HandleInboundFrame(frame)
}

func (h *BridgeHandler) handleInboundInflightExpiry(entry *inflight.InflightEntryB) {
	msgType := formatMsgType(entry.RequestType)
	connectionID := ""
	if entry.TCPReplyInfo != nil {
		connectionID = entry.TCPReplyInfo.ConnectionID
	}
	h.metrics.IncInboundOutcomes(connectionID, msgType, inboundStatusTimeout, inboundReasonInflightExpired)
}
