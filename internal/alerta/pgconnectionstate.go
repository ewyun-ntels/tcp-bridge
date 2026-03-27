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
func (c *Client) SendConnectionStateAlert(connID, endpoint, state string, priority int) {
	if !c.cfg.Enabled {
		return
	}
	if state != config.ConnStateReady && state != config.ConnStateConnectFailed {
		return
	}

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

	attrs := c.pgConnectionStateAttributes(connID, endpoint, state, priority, errCode)

	c.sendAlert(
		alertDef.Event,
		fmt.Sprintf("%s/%s", c.sysID, connID),
		severity,
		alertDef.Group,
		state,
		fmt.Sprintf("peer=%s state=%s connID=%s priority=%d", endpoint, state, connID, priority),
		attrs,
	)
}

// SendShutdownAlert sends a PGConnectionState DISCONNECTED alert for graceful shutdown.
// 종료 직전 유실을 막기 위해 동기 전송합니다.
func (c *Client) SendShutdownAlert(ctx context.Context, connID, endpoint string, priority int) error {
	if !c.cfg.Enabled {
		return nil
	}

	alertDef, ok := c.alertMap[pgConnectionStateEvent]
	if !ok {
		return nil
	}

	attrs := c.pgConnectionStateAttributes(
		connID,
		endpoint,
		config.ConnStateDisconnected,
		priority,
		alertDef.ErrCode,
	)

	return c.sendAlertSync(
		ctx,
		alertDef.Event,
		fmt.Sprintf("%s/%s", c.sysID, connID),
		"critical",
		alertDef.Group,
		config.ConnStateDisconnected,
		fmt.Sprintf("POD Down - connID=%s peer=%s priority=%d", connID, endpoint, priority),
		attrs,
	)
}

func (c *Client) pgConnectionStateAttributes(connID, endpoint, state string, priority int, errCode string) map[string]string {
	host, port, _ := net.SplitHostPort(endpoint)

	attrs := map[string]string{
		"sysId":    c.sysID,
		"connId":   connID,
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
