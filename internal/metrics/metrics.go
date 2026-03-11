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
	connectionState *prometheus.GaugeVec
	connectionCount prometheus.Counter

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
		[]string{"connection_type"},
	)

	m.connectionCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tcp_bridge_connection_total",
			Help: "Total number of connection attempts",
		},
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

	// Register all metrics
	prometheus.MustRegister(
		m.connectionState,
		m.connectionCount,
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

	mux.HandleFunc("/keylist", func(w http.ResponseWriter, r *http.Request) {
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
			m.logger.Error("failed to encode keylist response", "error", err)
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
func (m *Metrics) SetConnectionState(connType string, state string) {
	stateValue := 0.0
	switch state {
	case config.ConnStateConnecting:
		stateValue = 1.0
	case config.ConnStateReady:
		stateValue = 2.0
	}
	m.connectionState.WithLabelValues(connType).Set(stateValue)
}

func (m *Metrics) IncConnectionCount() {
	m.connectionCount.Inc()
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
