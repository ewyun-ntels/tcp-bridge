package metrics

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"tcp-bridge/internal/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all prometheus metrics
type Metrics struct {
	logger *slog.Logger
	config *config.MetricsConfig

	// Connection metrics
	connectionState              *prometheus.GaugeVec
	activeConnection             *prometheus.GaugeVec
	connectionAttemptsTotal      *prometheus.CounterVec
	connectionSuccessTotal       *prometheus.CounterVec
	connectionFailuresTotal      *prometheus.CounterVec
	disconnectsTotal             *prometheus.CounterVec
	pingRTTSeconds               *prometheus.HistogramVec
	lastActivityTimestampSeconds *prometheus.GaugeVec
	failoverTotal                *prometheus.CounterVec

	// TCP metrics
	tcpFramesSent       *prometheus.CounterVec
	tcpFramesReceived   *prometheus.CounterVec
	tcpBytesTransmitted *prometheus.CounterVec

	// NATS metrics
	natsMessagesProcessed *prometheus.CounterVec
	natsRequestDuration   *prometheus.HistogramVec

	// Queue metrics
	sendQueueSize         prometheus.Gauge
	sendQueueUtilization  prometheus.Gauge
	sendQueueEnqueueTotal prometheus.Counter
	sendQueueDropTotal    prometheus.Counter

	// Semaphore metrics
	semaphoreUtilization  *prometheus.GaugeVec
	semaphoreAcquireTotal *prometheus.CounterVec

	// Inflight metrics
	inflightEntries  *prometheus.GaugeVec
	inflightTimeouts *prometheus.CounterVec

	// Worker metrics
	workerTasksTotal   *prometheus.CounterVec
	workerTaskDuration *prometheus.HistogramVec
	workerErrors       *prometheus.CounterVec

	// Inbound traffic quality metrics
	inboundRequestsTotal            *prometheus.CounterVec
	inboundResponsesTotal           *prometheus.CounterVec
	inboundInflightRequests         *prometheus.GaugeVec
	inboundEndToEndDuration         *prometheus.HistogramVec
	inboundNATSRequestDuration      *prometheus.HistogramVec
	inboundResponseWriteDuration    *prometheus.HistogramVec
	inboundErrorsTotal              *prometheus.CounterVec
	inboundTimeoutTotal             *prometheus.CounterVec
	inboundUnmatchedResponseTotal   *prometheus.CounterVec
	inboundWorkerTasksTotal         *prometheus.CounterVec
	inboundQueueWaitDurationSeconds prometheus.Histogram

	// Outbound traffic quality metrics
	outboundRequestsTotal            *prometheus.CounterVec
	outboundResponsesTotal           *prometheus.CounterVec
	outboundInflightRequests         *prometheus.GaugeVec
	outboundEndToEndDuration         *prometheus.HistogramVec
	outboundTCPResponseWaitDuration  *prometheus.HistogramVec
	outboundNATSReplyDuration        *prometheus.HistogramVec
	outboundErrorsTotal              *prometheus.CounterVec
	outboundTimeoutTotal             *prometheus.CounterVec
	outboundUnmatchedResponseTotal   *prometheus.CounterVec
	outboundWorkerTasksTotal         *prometheus.CounterVec
	outboundQueueWaitDurationSeconds prometheus.Histogram

	// HTTP server
	server *http.Server

	keyListProvider func() []json.RawMessage
}

// NewMetrics creates new metrics instance
func NewMetrics(logger *slog.Logger, cfg *config.MetricsConfig) *Metrics {
	m := &Metrics{
		logger: logger,
		config: cfg,
	}

	m.initMetrics()
	return m
}

// SetKeyListProvider configures the data source for the /keylist endpoint.
func (m *Metrics) SetKeyListProvider(provider func() []json.RawMessage) {
	m.keyListProvider = provider
}

// initMetrics initializes all prometheus metrics
func (m *Metrics) initMetrics() {
	// Connection metrics
	m.connectionState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_connection_state",
			Help: "Connection state (0=disconnected, 1=connecting, 2=ready)",
		},
		[]string{"connection_id", "endpoint"},
	)

	m.activeConnection = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_active_connection",
			Help: "Whether the connection is currently selected as active (1=active, 0=inactive)",
		},
		[]string{"connection_id"},
	)

	m.connectionAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_connection_attempts_total",
			Help: "Total number of connection attempts",
		},
		[]string{"connection_id"},
	)

	m.connectionSuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_connection_success_total",
			Help: "Total number of successful connections",
		},
		[]string{"connection_id"},
	)

	m.connectionFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_connection_failures_total",
			Help: "Total number of connection failures by reason",
		},
		[]string{"connection_id", "reason"},
	)

	m.disconnectsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_disconnects_total",
			Help: "Total number of disconnections by reason",
		},
		[]string{"connection_id", "reason"},
	)

	m.pingRTTSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_ping_rtt_seconds",
			Help:    "Observed TCP ping round-trip time in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"connection_id"},
	)

	m.lastActivityTimestampSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_last_activity_timestamp_seconds",
			Help: "Unix timestamp of the last observed connection activity",
		},
		[]string{"connection_id"},
	)

	m.failoverTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_failover_total",
			Help: "Total number of active connection changes by reason",
		},
		[]string{"from", "to", "reason"},
	)

	// TCP metrics
	m.tcpFramesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_tcp_frames_sent_total",
			Help: "Total number of TCP frames sent",
		},
		[]string{"frame_type", "target"},
	)

	m.tcpFramesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_tcp_frames_received_total",
			Help: "Total number of TCP frames received",
		},
		[]string{"frame_type"},
	)

	m.tcpBytesTransmitted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_tcp_bytes_transmitted_total",
			Help: "Total number of bytes transmitted over TCP",
		},
		[]string{"direction"},
	)

	// NATS metrics
	m.natsMessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_nats_messages_processed_total",
			Help: "Total number of NATS messages processed",
		},
		[]string{"direction", "status"},
	)

	m.natsRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_nats_request_duration_seconds",
			Help:    "NATS request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"direction"},
	)

	// Queue metrics
	m.sendQueueSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_send_queue_size",
			Help: "Current send queue size",
		},
	)

	m.sendQueueUtilization = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_send_queue_utilization_percent",
			Help: "Send queue utilization percentage",
		},
	)

	m.sendQueueEnqueueTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tcp_bridge_send_queue_enqueue_total",
			Help: "Total number of items enqueued to send queue",
		},
	)

	m.sendQueueDropTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tcp_bridge_send_queue_drop_total",
			Help: "Total number of items dropped from send queue",
		},
	)

	// Semaphore metrics
	m.semaphoreUtilization = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_semaphore_utilization_percent",
			Help: "Semaphore utilization percentage",
		},
		[]string{"semaphore"},
	)

	m.semaphoreAcquireTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_semaphore_acquire_total",
			Help: "Total number of semaphore acquisitions",
		},
		[]string{"semaphore", "status"},
	)

	// Inflight metrics
	m.inflightEntries = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_inflight_entries",
			Help: "Number of inflight entries",
		},
		[]string{"type"},
	)

	m.inflightTimeouts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inflight_timeouts_total",
			Help: "Total number of inflight timeouts",
		},
		[]string{"type"},
	)

	// Worker metrics
	m.workerTasksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_worker_tasks_total",
			Help: "Total number of worker tasks processed",
		},
		[]string{"pool", "status"},
	)

	m.workerTaskDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_worker_task_duration_seconds",
			Help:    "Worker task duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"pool"},
	)

	m.workerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_worker_errors_total",
			Help: "Total number of worker errors",
		},
		[]string{"pool", "error_type"},
	)

	m.inboundRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_requests_total",
			Help: "Total number of inbound TCP requests received",
		},
		[]string{"msg_type"},
	)

	m.inboundResponsesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_responses_total",
			Help: "Total number of completed inbound request outcomes",
		},
		[]string{"msg_type", "status"},
	)

	m.inboundInflightRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_inbound_inflight_requests",
			Help: "Current number of inflight inbound requests",
		},
		[]string{"msg_type"},
	)

	m.inboundEndToEndDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_inbound_end_to_end_duration_seconds",
			Help:    "End-to-end duration from inbound TCP request receipt to final TCP response handling",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"msg_type", "status"},
	)

	m.inboundNATSRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_inbound_nats_request_duration_seconds",
			Help:    "Duration spent waiting for the NATS request/reply round trip",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"msg_type", "status"},
	)

	m.inboundResponseWriteDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_inbound_response_write_duration_seconds",
			Help:    "Duration spent writing inbound TCP responses back to the source connection",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"msg_type", "status"},
	)

	m.inboundErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_errors_total",
			Help: "Total number of inbound processing errors by type",
		},
		[]string{"msg_type", "error_type"},
	)

	m.inboundTimeoutTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_timeout_total",
			Help: "Total number of inbound timeouts by stage",
		},
		[]string{"msg_type", "stage"},
	)

	m.inboundUnmatchedResponseTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_unmatched_response_total",
			Help: "Total number of inbound requests that expired without a matched completion",
		},
		[]string{"msg_type"},
	)

	m.inboundWorkerTasksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_worker_tasks_total",
			Help: "Total number of inbound worker tasks processed",
		},
		[]string{"status"},
	)

	m.inboundQueueWaitDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_inbound_queue_wait_duration_seconds",
			Help:    "Time spent waiting in the inbound worker queue before processing starts",
			Buckets: prometheus.DefBuckets,
		},
	)

	m.outboundRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_requests_total",
			Help: "Total number of outbound NATS requests received",
		},
		[]string{"subject", "msg_type"},
	)

	m.outboundResponsesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_responses_total",
			Help: "Total number of completed outbound request outcomes",
		},
		[]string{"subject", "msg_type", "status"},
	)

	m.outboundInflightRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_outbound_inflight_requests",
			Help: "Current number of inflight outbound requests",
		},
		[]string{"subject", "msg_type"},
	)

	m.outboundEndToEndDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_outbound_end_to_end_duration_seconds",
			Help:    "End-to-end duration from outbound NATS request receipt to final NATS reply publish",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"subject", "msg_type", "status"},
	)

	m.outboundTCPResponseWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_outbound_tcp_response_wait_duration_seconds",
			Help:    "Duration spent waiting for the TCP response after an outbound send",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"subject", "msg_type", "status"},
	)

	m.outboundNATSReplyDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_outbound_nats_reply_duration_seconds",
			Help:    "Duration spent publishing the outbound NATS reply",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"subject", "msg_type", "status"},
	)

	m.outboundErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_errors_total",
			Help: "Total number of outbound processing errors by type",
		},
		[]string{"subject", "msg_type", "error_type"},
	)

	m.outboundTimeoutTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_timeout_total",
			Help: "Total number of outbound timeouts by stage",
		},
		[]string{"subject", "msg_type", "stage"},
	)

	m.outboundUnmatchedResponseTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_unmatched_response_total",
			Help: "Total number of outbound TCP responses that could not be matched to an inflight request",
		},
		[]string{"msg_type"},
	)

	m.outboundWorkerTasksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_worker_tasks_total",
			Help: "Total number of outbound worker tasks processed",
		},
		[]string{"status"},
	)

	m.outboundQueueWaitDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_outbound_queue_wait_duration_seconds",
			Help:    "Time spent waiting in the outbound worker queue before processing starts",
			Buckets: prometheus.DefBuckets,
		},
	)

	// Register all metrics
	prometheus.MustRegister(
		m.connectionState,
		m.activeConnection,
		m.connectionAttemptsTotal,
		m.connectionSuccessTotal,
		m.connectionFailuresTotal,
		m.disconnectsTotal,
		m.pingRTTSeconds,
		m.lastActivityTimestampSeconds,
		m.failoverTotal,
		m.tcpFramesSent,
		m.tcpFramesReceived,
		m.tcpBytesTransmitted,
		m.natsMessagesProcessed,
		m.natsRequestDuration,
		m.sendQueueSize,
		m.sendQueueUtilization,
		m.sendQueueEnqueueTotal,
		m.sendQueueDropTotal,
		m.semaphoreUtilization,
		m.semaphoreAcquireTotal,
		m.inflightEntries,
		m.inflightTimeouts,
		m.workerTasksTotal,
		m.workerTaskDuration,
		m.workerErrors,
		m.inboundRequestsTotal,
		m.inboundResponsesTotal,
		m.inboundInflightRequests,
		m.inboundEndToEndDuration,
		m.inboundNATSRequestDuration,
		m.inboundResponseWriteDuration,
		m.inboundErrorsTotal,
		m.inboundTimeoutTotal,
		m.inboundUnmatchedResponseTotal,
		m.inboundWorkerTasksTotal,
		m.inboundQueueWaitDurationSeconds,
		m.outboundRequestsTotal,
		m.outboundResponsesTotal,
		m.outboundInflightRequests,
		m.outboundEndToEndDuration,
		m.outboundTCPResponseWaitDuration,
		m.outboundNATSReplyDuration,
		m.outboundErrorsTotal,
		m.outboundTimeoutTotal,
		m.outboundUnmatchedResponseTotal,
		m.outboundWorkerTasksTotal,
		m.outboundQueueWaitDurationSeconds,
	)
}

// Start starts the metrics HTTP server
func (m *Metrics) Start() error {
	if !m.config.Enabled {
		m.logger.Info("metrics disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle(m.config.Path, promhttp.Handler())

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		responses := []json.RawMessage{}
		if m.keyListProvider != nil {
			responses = m.keyListProvider()
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(responses); err != nil {
			m.logger.Error("failed to encode hello response", "error", err)
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
		}
	})

	m.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", m.config.Port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		m.logger.Info("starting metrics server", "address", m.server.Addr, "path", m.config.Path)
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Error("metrics server error", "error", err)
		}
	}()

	return nil
}

// Stop stops the metrics HTTP server
func (m *Metrics) Stop() error {
	if m.server != nil {
		m.logger.Info("stopping metrics server")
		return m.server.Close()
	}
	return nil
}

// Connection metrics methods
func (m *Metrics) SetConnectionState(connectionID string, endpoint string, state string) {
	stateValue := 0.0
	switch state {
	case config.ConnStateConnecting:
		stateValue = 1.0
	case config.ConnStateReady:
		stateValue = 2.0
	}
	m.connectionState.WithLabelValues(connectionID, endpoint).Set(stateValue)
}

func (m *Metrics) SetActiveConnection(connectionIDs []string, activeConnectionID string) {
	for _, connectionID := range connectionIDs {
		m.activeConnection.WithLabelValues(connectionID).Set(0)
	}
	if activeConnectionID != "" {
		m.activeConnection.WithLabelValues(activeConnectionID).Set(1)
	}
}

func (m *Metrics) IncConnectionAttempts(connectionID string) {
	m.connectionAttemptsTotal.WithLabelValues(connectionID).Inc()
}

func (m *Metrics) IncConnectionSuccess(connectionID string) {
	m.connectionSuccessTotal.WithLabelValues(connectionID).Inc()
}

func (m *Metrics) IncConnectionFailure(connectionID string, reason string) {
	m.connectionFailuresTotal.WithLabelValues(connectionID, reason).Inc()
}

func (m *Metrics) IncDisconnect(connectionID string, reason string) {
	m.disconnectsTotal.WithLabelValues(connectionID, reason).Inc()
}

func (m *Metrics) ObservePingRTT(connectionID string, duration time.Duration) {
	m.pingRTTSeconds.WithLabelValues(connectionID).Observe(duration.Seconds())
}

func (m *Metrics) SetLastActivityTimestamp(connectionID string, timestamp time.Time) {
	if timestamp.IsZero() {
		m.lastActivityTimestampSeconds.WithLabelValues(connectionID).Set(0)
		return
	}
	m.lastActivityTimestampSeconds.WithLabelValues(connectionID).Set(float64(timestamp.Unix()))
}

func (m *Metrics) IncFailover(from string, to string, reason string) {
	if from == "" {
		from = "none"
	}
	if to == "" {
		to = "none"
	}
	m.failoverTotal.WithLabelValues(from, to, reason).Inc()
}

// TCP metrics methods
func (m *Metrics) IncTCPFramesSent(frameType string, target string) {
	m.tcpFramesSent.WithLabelValues(frameType, target).Inc()
}

func (m *Metrics) IncTCPFramesReceived(frameType string) {
	m.tcpFramesReceived.WithLabelValues(frameType).Inc()
}

func (m *Metrics) AddTCPBytesTransmitted(direction string, bytes int) {
	m.tcpBytesTransmitted.WithLabelValues(direction).Add(float64(bytes))
}

// NATS metrics methods
func (m *Metrics) IncNATSMessagesProcessed(direction string, status string) {
	m.natsMessagesProcessed.WithLabelValues(direction, status).Inc()
}

func (m *Metrics) ObserveNATSRequestDuration(direction string, duration time.Duration) {
	m.natsRequestDuration.WithLabelValues(direction).Observe(duration.Seconds())
}

// Queue metrics methods
func (m *Metrics) SetSendQueueSize(size int) {
	m.sendQueueSize.Set(float64(size))
}

func (m *Metrics) SetSendQueueUtilization(percent float64) {
	m.sendQueueUtilization.Set(percent)
}

func (m *Metrics) IncSendQueueEnqueue() {
	m.sendQueueEnqueueTotal.Inc()
}

func (m *Metrics) IncSendQueueDrop() {
	m.sendQueueDropTotal.Inc()
}

// Semaphore metrics methods
func (m *Metrics) SetSemaphoreUtilization(semaphore string, percent float64) {
	m.semaphoreUtilization.WithLabelValues(semaphore).Set(percent)
}

func (m *Metrics) IncSemaphoreAcquire(semaphore string, status string) {
	m.semaphoreAcquireTotal.WithLabelValues(semaphore, status).Inc()
}

// Inflight metrics methods
func (m *Metrics) SetInflightEntries(inflightType string, count int) {
	m.inflightEntries.WithLabelValues(inflightType).Set(float64(count))
}

func (m *Metrics) IncInflightTimeouts(inflightType string) {
	m.inflightTimeouts.WithLabelValues(inflightType).Inc()
}

// Worker metrics methods
func (m *Metrics) IncWorkerTasks(pool string, status string) {
	m.workerTasksTotal.WithLabelValues(pool, status).Inc()
}

func (m *Metrics) ObserveWorkerTaskDuration(pool string, duration time.Duration) {
	m.workerTaskDuration.WithLabelValues(pool).Observe(duration.Seconds())
}

func (m *Metrics) IncWorkerErrors(pool string, errorType string) {
	m.workerErrors.WithLabelValues(pool, errorType).Inc()
}

func (m *Metrics) IncInboundRequests(msgType string) {
	m.inboundRequestsTotal.WithLabelValues(msgType).Inc()
}

func (m *Metrics) IncInboundResponses(msgType string, status string) {
	m.inboundResponsesTotal.WithLabelValues(msgType, status).Inc()
}

func (m *Metrics) AddInboundInflight(msgType string, delta int) {
	m.inboundInflightRequests.WithLabelValues(msgType).Add(float64(delta))
}

func (m *Metrics) ObserveInboundEndToEndDuration(msgType string, status string, duration time.Duration) {
	m.inboundEndToEndDuration.WithLabelValues(msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) ObserveInboundNATSRequestDuration(msgType string, status string, duration time.Duration) {
	m.inboundNATSRequestDuration.WithLabelValues(msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) ObserveInboundResponseWriteDuration(msgType string, status string, duration time.Duration) {
	m.inboundResponseWriteDuration.WithLabelValues(msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) IncInboundErrors(msgType string, errorType string) {
	m.inboundErrorsTotal.WithLabelValues(msgType, errorType).Inc()
}

func (m *Metrics) IncInboundTimeout(msgType string, stage string) {
	m.inboundTimeoutTotal.WithLabelValues(msgType, stage).Inc()
}

func (m *Metrics) IncInboundUnmatchedResponse(msgType string) {
	m.inboundUnmatchedResponseTotal.WithLabelValues(msgType).Inc()
}

func (m *Metrics) IncInboundWorkerTasks(status string) {
	m.inboundWorkerTasksTotal.WithLabelValues(status).Inc()
}

func (m *Metrics) ObserveInboundQueueWaitDuration(duration time.Duration) {
	m.inboundQueueWaitDurationSeconds.Observe(duration.Seconds())
}

func (m *Metrics) IncOutboundRequests(subject string, msgType string) {
	m.outboundRequestsTotal.WithLabelValues(subject, msgType).Inc()
}

func (m *Metrics) IncOutboundResponses(subject string, msgType string, status string) {
	m.outboundResponsesTotal.WithLabelValues(subject, msgType, status).Inc()
}

func (m *Metrics) AddOutboundInflight(subject string, msgType string, delta int) {
	m.outboundInflightRequests.WithLabelValues(subject, msgType).Add(float64(delta))
}

func (m *Metrics) ObserveOutboundEndToEndDuration(subject string, msgType string, status string, duration time.Duration) {
	m.outboundEndToEndDuration.WithLabelValues(subject, msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) ObserveOutboundTCPResponseWaitDuration(subject string, msgType string, status string, duration time.Duration) {
	m.outboundTCPResponseWaitDuration.WithLabelValues(subject, msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) ObserveOutboundNATSReplyDuration(subject string, msgType string, status string, duration time.Duration) {
	m.outboundNATSReplyDuration.WithLabelValues(subject, msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) IncOutboundErrors(subject string, msgType string, errorType string) {
	m.outboundErrorsTotal.WithLabelValues(subject, msgType, errorType).Inc()
}

func (m *Metrics) IncOutboundTimeout(subject string, msgType string, stage string) {
	m.outboundTimeoutTotal.WithLabelValues(subject, msgType, stage).Inc()
}

func (m *Metrics) IncOutboundUnmatchedResponse(msgType string) {
	m.outboundUnmatchedResponseTotal.WithLabelValues(msgType).Inc()
}

func (m *Metrics) IncOutboundWorkerTasks(status string) {
	m.outboundWorkerTasksTotal.WithLabelValues(status).Inc()
}

func (m *Metrics) ObserveOutboundQueueWaitDuration(duration time.Duration) {
	m.outboundQueueWaitDurationSeconds.Observe(duration.Seconds())
}
