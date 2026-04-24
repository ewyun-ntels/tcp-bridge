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

	connectionNameResolver func(connectionID string) string

	// Connection metrics
	connectionState  *prometheus.GaugeVec
	activeConnection *prometheus.GaugeVec

	// Inbound traffic quality metrics
	inboundRequestsTotal      *prometheus.CounterVec
	inboundOutcomesTotal      *prometheus.CounterVec
	inboundResponsesSentTotal *prometheus.CounterVec
	inboundEndToEndDuration   *prometheus.HistogramVec

	// Outbound traffic quality metrics
	outboundRequestsTotal      *prometheus.CounterVec
	outboundOutcomesTotal      *prometheus.CounterVec
	outboundResponsesSentTotal *prometheus.CounterVec
	outboundEndToEndDuration   *prometheus.HistogramVec

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

// SetConnectionNameResolver configures an optional connID -> display name resolver.
func (m *Metrics) SetConnectionNameResolver(resolver func(connectionID string) string) {
	m.connectionNameResolver = resolver
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
		[]string{"connection_id", "connection_name", "endpoint"},
	)

	m.activeConnection = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tcp_bridge_selected_connection",
			Help: "Whether the connection is currently selected as primary/master (1=selected, 0=not selected)",
		},
		[]string{"connection_id", "connection_name"},
	)

	m.inboundRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_requests_total",
			Help: "Total number of inbound TCP requests received",
		},
		[]string{"connection_id", "connection_name", "msg_type"},
	)

	m.inboundOutcomesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_outcomes_total",
			Help: "Total number of completed inbound request outcomes",
		},
		[]string{"connection_id", "connection_name", "msg_type", "status", "reason"},
	)

	m.inboundResponsesSentTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_inbound_responses_sent_total",
			Help: "Total number of inbound TCP responses successfully sent to the external peer",
		},
		[]string{"connection_id", "connection_name", "msg_type", "result"},
	)

	m.inboundEndToEndDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_inbound_end_to_end_duration_seconds",
			Help:    "End-to-end duration from inbound TCP request receipt to final TCP response handling",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"connection_id", "connection_name", "msg_type", "status"},
	)

	m.outboundRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_requests_total",
			Help: "Total number of outbound NATS requests received",
		},
		[]string{"connection_id", "connection_name", "subject", "msg_type"},
	)

	m.outboundOutcomesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_outcomes_total",
			Help: "Total number of completed outbound request outcomes",
		},
		[]string{"connection_id", "connection_name", "subject", "msg_type", "status", "reason"},
	)

	m.outboundResponsesSentTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tcp_bridge_outbound_responses_sent_total",
			Help: "Total number of outbound NATS replies successfully sent to the requester",
		},
		[]string{"connection_id", "connection_name", "subject", "msg_type", "result"},
	)

	m.outboundEndToEndDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tcp_bridge_outbound_end_to_end_duration_seconds",
			Help:    "End-to-end duration from outbound NATS request receipt to final NATS reply publish",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"connection_id", "connection_name", "subject", "msg_type", "status"},
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
		m.inboundOutcomesTotal,
		m.inboundResponsesSentTotal,
		m.inboundEndToEndDuration,
		m.outboundRequestsTotal,
		m.outboundOutcomesTotal,
		m.outboundResponsesSentTotal,
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
	normalizedID, connectionName := m.connectionLabels(connectionID)
	stateValue := 0.0
	switch state {
	case config.ConnStateConnecting:
		stateValue = 1.0
	case config.ConnStateReady:
		stateValue = 2.0
	}
	m.connectionState.WithLabelValues(normalizedID, connectionName, endpoint).Set(stateValue)
}

func (m *Metrics) SetActiveConnection(connectionIDs []string, activeConnectionID string) {
	for _, connectionID := range connectionIDs {
		normalizedID, connectionName := m.connectionLabels(connectionID)
		m.activeConnection.WithLabelValues(normalizedID, connectionName).Set(0)
	}
	if activeConnectionID != "" {
		normalizedID, connectionName := m.connectionLabels(activeConnectionID)
		m.activeConnection.WithLabelValues(normalizedID, connectionName).Set(1)
	}
}

func (m *Metrics) IncInboundRequests(connectionID string, msgType string) {
	normalizedID, connectionName := m.connectionLabels(connectionID)
	m.inboundRequestsTotal.WithLabelValues(normalizedID, connectionName, msgType).Inc()
}

func (m *Metrics) IncInboundOutcomes(connectionID string, msgType string, status string, reason string) {
	normalizedID, connectionName := m.connectionLabels(connectionID)
	m.inboundOutcomesTotal.WithLabelValues(normalizedID, connectionName, msgType, status, reason).Inc()
}

func (m *Metrics) IncInboundResponsesSent(connectionID string, msgType string, result string) {
	normalizedID, connectionName := m.connectionLabels(connectionID)
	m.inboundResponsesSentTotal.WithLabelValues(normalizedID, connectionName, msgType, result).Inc()
}

func (m *Metrics) ObserveInboundEndToEndDuration(connectionID string, msgType string, status string, duration time.Duration) {
	normalizedID, connectionName := m.connectionLabels(connectionID)
	m.inboundEndToEndDuration.WithLabelValues(normalizedID, connectionName, msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) IncOutboundRequests(connectionID string, subject string, msgType string) {
	normalizedID, connectionName := m.connectionLabels(connectionID)
	m.outboundRequestsTotal.WithLabelValues(normalizedID, connectionName, subject, msgType).Inc()
}

func (m *Metrics) IncOutboundOutcomes(connectionID string, subject string, msgType string, status string, reason string) {
	normalizedID, connectionName := m.connectionLabels(connectionID)
	m.outboundOutcomesTotal.WithLabelValues(normalizedID, connectionName, subject, msgType, status, reason).Inc()
}

func (m *Metrics) IncOutboundResponsesSent(connectionID string, subject string, msgType string, result string) {
	normalizedID, connectionName := m.connectionLabels(connectionID)
	m.outboundResponsesSentTotal.WithLabelValues(normalizedID, connectionName, subject, msgType, result).Inc()
}

func (m *Metrics) ObserveOutboundEndToEndDuration(connectionID string, subject string, msgType string, status string, duration time.Duration) {
	normalizedID, connectionName := m.connectionLabels(connectionID)
	m.outboundEndToEndDuration.WithLabelValues(normalizedID, connectionName, subject, msgType, status).Observe(duration.Seconds())
}

func (m *Metrics) connectionLabels(connectionID string) (string, string) {
	normalizedID := normalizeConnectionID(connectionID)
	return normalizedID, m.resolveConnectionName(normalizedID)
}

func (m *Metrics) resolveConnectionName(connectionID string) string {
	if m.connectionNameResolver != nil {
		if name := m.connectionNameResolver(connectionID); name != "" {
			return name
		}
	}
	return normalizeConnectionID(connectionID)
}

func normalizeConnectionID(connectionID string) string {
	if connectionID == "" {
		return "unknown"
	}
	return connectionID
}
