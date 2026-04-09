package metrics

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"tcp-bridge/internal/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all prometheus metrics
type Metrics struct {
	logger   *slog.Logger
	config   *config.MetricsConfig
	registry *prometheus.Registry

	// Connection metrics
	connectionState  *prometheus.GaugeVec
	activeConnection *prometheus.GaugeVec

	// Inbound traffic quality metrics
	inboundRequestsTotal    *prometheus.CounterVec
	inboundResponsesTotal   *prometheus.CounterVec
	inboundEndToEndDuration *prometheus.HistogramVec

	// Outbound traffic quality metrics
	outboundRequestsTotal    *prometheus.CounterVec
	outboundResponsesTotal   *prometheus.CounterVec
	outboundEndToEndDuration *prometheus.HistogramVec

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
	m.registry = prometheus.NewRegistry()

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
			Name: "tcp_bridge_selected_connection",
			Help: "Whether the connection is currently selected as primary/master (1=selected, 0=not selected)",
		},
		[]string{"connection_id"},
	)

	m.inboundRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_requests_total",
			Help: "Total number of inbound TCP requests received",
		},
		[]string{"connection_id", "msg_type"},
	)

	m.inboundResponsesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_responses_total",
			Help: "Total number of completed inbound request outcomes",
		},
		[]string{"connection_id", "msg_type", "status"},
	)

	m.inboundEndToEndDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_inbound_end_to_end_duration_seconds",
			Help:    "End-to-end duration from inbound TCP request receipt to final TCP response handling",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"connection_id", "msg_type", "status"},
	)

	m.outboundRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_requests_total",
			Help: "Total number of outbound NATS requests received",
		},
		[]string{"connection_id", "subject", "msg_type"},
	)

	m.outboundResponsesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_responses_total",
			Help: "Total number of completed outbound request outcomes",
		},
		[]string{"connection_id", "subject", "msg_type", "status"},
	)

	m.outboundEndToEndDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_outbound_end_to_end_duration_seconds",
			Help:    "End-to-end duration from outbound NATS request receipt to final NATS reply publish",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"connection_id", "subject", "msg_type", "status"},
	)

	if m.config.IncludeDefaultMetrics {
		m.registry.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
	}

	// Register all application metrics
	m.registry.MustRegister(
		m.connectionState,
		m.activeConnection,
		m.inboundRequestsTotal,
		m.inboundResponsesTotal,
		m.inboundEndToEndDuration,
		m.outboundRequestsTotal,
		m.outboundResponsesTotal,
		m.outboundEndToEndDuration,
	)
}

// Start starts the metrics HTTP server
func (m *Metrics) Start() error {
	if !m.config.Enabled {
		m.logger.Info("metrics disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle(m.config.Path, promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))

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

func (m *Metrics) IncInboundRequests(connectionID string, msgType string) {
	m.inboundRequestsTotal.WithLabelValues(normalizeConnectionID(connectionID), msgType).Inc()
}

func (m *Metrics) IncInboundResponses(connectionID string, msgType string, status string) {
	m.inboundResponsesTotal.WithLabelValues(normalizeConnectionID(connectionID), msgType, status).Inc()
}

func (m *Metrics) ObserveInboundEndToEndDuration(connectionID string, msgType string, status string, duration time.Duration) {
	m.inboundEndToEndDuration.WithLabelValues(normalizeConnectionID(connectionID), msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) IncOutboundRequests(connectionID string, subject string, msgType string) {
	m.outboundRequestsTotal.WithLabelValues(normalizeConnectionID(connectionID), subject, msgType).Inc()
}

func (m *Metrics) IncOutboundResponses(connectionID string, subject string, msgType string, status string) {
	m.outboundResponsesTotal.WithLabelValues(normalizeConnectionID(connectionID), subject, msgType, status).Inc()
}

func (m *Metrics) ObserveOutboundEndToEndDuration(connectionID string, subject string, msgType string, status string, duration time.Duration) {
	m.outboundEndToEndDuration.WithLabelValues(normalizeConnectionID(connectionID), subject, msgType, status).Observe(duration.Seconds())
}

func normalizeConnectionID(connectionID string) string {
	if connectionID == "" {
		return "unknown"
	}
	return connectionID
}
