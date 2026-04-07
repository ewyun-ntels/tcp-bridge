package alerta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"tcp-bridge/internal/config"
)

// 2026-03-27: Alerta 알람 클라이언트 추가
// - service: sysID에서 자동 생성
// - tags: event, group에서 자동 생성
// - severity: READY→normal, 나머지→critical

// alertPayload represents the JSON body sent to Alerta API
type alertPayload struct {
	Resource    string            `json:"resource"`
	Event       string            `json:"event"`
	Environment string            `json:"environment"`
	Severity    string            `json:"severity"`
	Service     []string          `json:"service"`
	Group       string            `json:"group"`
	Value       string            `json:"value"`
	Text        string            `json:"text"`
	Tags        []string          `json:"tags"`
	Attributes  map[string]string `json:"attributes"`
}

// Client sends alerts to the Alerta API
type Client struct {
	logger           *slog.Logger
	cfg              *config.AlertaConfig
	sysID            string
	httpClient       *http.Client
	alertMap         map[string]config.AlertDef // event name → AlertDef
	mu               sync.Mutex
	lastAlertedState map[string]string // connID → last sent alert state
}

// NewClient creates a new Alerta client
func NewClient(logger *slog.Logger, cfg *config.AlertaConfig, sysID string) *Client {
	alertMap := make(map[string]config.AlertDef, len(cfg.Alerts))
	for _, a := range cfg.Alerts {
		alertMap[a.Event] = a
	}

	return &Client{
		logger: logger,
		cfg:    cfg,
		sysID:  sysID,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		alertMap:         alertMap,
		lastAlertedState: make(map[string]string),
	}
}

// sendAlert posts an alert to the Alerta API asynchronously.
// service는 sysID, tags는 event+group에서 자동 생성
func (c *Client) sendAlert(event, resource, severity, group, value, text string, attrs map[string]string) {
	go func() {
		if err := c.sendAlertSync(context.Background(), event, resource, severity, group, value, text, attrs); err != nil {
			c.logger.Warn("failed to send alerta alert",
				"error", err,
				"event", event,
				"resource", resource,
				"severity", severity)
		}
	}()
}

// sendAlertSync posts an alert to the Alerta API synchronously.
func (c *Client) sendAlertSync(ctx context.Context, event, resource, severity, group, value, text string, attrs map[string]string) error {
	payload := alertPayload{
		Resource:    resource,
		Event:       event,
		Environment: c.cfg.Environment,
		Severity:    severity,
		Service:     []string{c.sysID},
		Group:       group,
		Value:       value,
		Text:        text,
		Tags:        []string{c.sysID, event, group},
		Attributes:  attrs,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal alerta payload: %w", err)
	}

	c.logger.Info("sending alerta alert",
		"url", c.cfg.URL,
		"event", event,
		"resource", resource,
		"severity", severity,
		"value", value)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create alerta request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alerta returned status %d", resp.StatusCode)
	}

	c.logger.Debug("alerta alert sent",
		"event", event,
		"resource", resource,
		"severity", severity,
		"value", value)
	return nil
}

// SetSysID updates the sysID (called after SysID is generated)
func (c *Client) SetSysID(sysID string) {
	c.sysID = sysID
}

// FindAlert returns the AlertDef for a given event name
func (c *Client) FindAlert(event string) (config.AlertDef, bool) {
	def, ok := c.alertMap[event]
	return def, ok
}

// SendTestAlert sends a test alert to verify connectivity (동기 호출)
func (c *Client) SendTestAlert() error {
	if !c.cfg.Enabled {
		return nil
	}

	payload := alertPayload{
		Resource:    c.sysID + "/test",
		Event:       "TestAlert",
		Environment: c.cfg.Environment,
		Severity:    "informational",
		Service:     []string{c.sysID},
		Group:       "Test",
		Value:       "test",
		Text:        fmt.Sprintf("tcp-bridge alerta connectivity test at %s", time.Now().Format(time.RFC3339)),
		Tags:        []string{c.sysID, "TestAlert", "Test"},
		Attributes:  map[string]string{"sysId": c.sysID},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal test alert: %w", err)
	}

	resp, err := c.httpClient.Post(c.cfg.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to send test alert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alerta returned status %d", resp.StatusCode)
	}

	return nil
}
