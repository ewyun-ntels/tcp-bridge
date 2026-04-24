package alerta

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"tcp-bridge/internal/config"
)

const pgConnectionStateEvent = "PGConnectionState"

// SendConnectionStateAlert sends a PGConnectionState alert for TCP connection state changes.
// READY → severity "normal" (errCode 비포함), 나머지 → severity "critical" (errCode 포함)
// 비동기(goroutine)로 전송하여 메인 흐름을 블로킹하지 않습니다.
func (c *Client) SendConnectionStateAlert(connID, connName, endpoint, state string, priority int) {
	if !c.cfg.Enabled {
		return
	}
	if state != config.ConnStateReady && state != config.ConnStateConnectFailed {
		return
	}

	c.mu.Lock()
	if c.lastAlertedState[connID] == state {
		c.mu.Unlock()
		return
	}
	c.lastAlertedState[connID] = state
	c.mu.Unlock()

	alertDef, ok := c.alertMap[pgConnectionStateEvent]
	if !ok {
		return
	}

	severity := "critical"
	errCode := alertDef.ErrCode
	if state == config.ConnStateReady {
		severity = "normal"
		errCode = ""
	}

	displayName := alertConnectionName(connName, connID)
	attrs := c.pgConnectionStateAttributes(connID, displayName, endpoint, state, priority, errCode)

	c.sendAlert(
		alertDef.Event,
		displayName,
		severity,
		alertDef.Group,
		state,
		fmt.Sprintf("peer=%s state=%s connection=%s priority=%d", endpoint, state, displayName, priority),
		attrs,
	)
}

// SendShutdownAlert sends a PGConnectionState DISCONNECTED alert for graceful shutdown.
// 종료 직전 유실을 막기 위해 동기 전송합니다.
func (c *Client) SendShutdownAlert(ctx context.Context, connID, connName, endpoint string, priority int) error {
	if !c.cfg.Enabled {
		return nil
	}

	alertDef, ok := c.alertMap[pgConnectionStateEvent]
	if !ok {
		return nil
	}

	displayName := alertConnectionName(connName, connID)
	attrs := c.pgConnectionStateAttributes(
		connID,
		displayName,
		endpoint,
		config.ConnStateDisconnected,
		priority,
		alertDef.ErrCode,
	)

	return c.sendAlertSync(
		ctx,
		alertDef.Event,
		displayName,
		"critical",
		alertDef.Group,
		config.ConnStateDisconnected,
		fmt.Sprintf("POD Down - connection=%s peer=%s priority=%d", displayName, endpoint, priority),
		attrs,
	)
}

func (c *Client) pgConnectionStateAttributes(connID, connName, endpoint, state string, priority int, errCode string) map[string]string {
	host, port, _ := net.SplitHostPort(endpoint)

	attrs := map[string]string{
		"sysId":    c.sysID,
		"connId":   connID,
		"connName": connName,
		"peerHost": host,
		"peerPort": port,
		"priority": strconv.Itoa(priority),
		"state":    state,
	}
	if errCode != "" {
		attrs["errCode"] = errCode
	}

	return attrs
}

func alertConnectionName(connName, connID string) string {
	if connName != "" {
		return connName
	}
	if connID != "" {
		return connID
	}
	return "unknown"
}
