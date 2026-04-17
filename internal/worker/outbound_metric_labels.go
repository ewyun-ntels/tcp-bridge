package worker

const (
	outboundStatusSuccess = "success"
	outboundStatusError   = "error"
	outboundStatusTimeout = "timeout"
	outboundStatusDropped = "dropped"
)

const (
	outboundReasonOK                    = "ok"
	outboundReasonQueueFull             = "queue_full"
	outboundReasonMissingReplySubject   = "missing_reply_subject"
	outboundReasonUnknownSubject        = "unknown_subject"
	outboundReasonFrameSerializeFailed  = "frame_serialize_failed"
	outboundReasonNoReadyConnection     = "no_ready_connection"
	outboundReasonTCPRequestWriteFailed = "tcp_request_write_failed"
	outboundReasonTCPResponseTimeout    = "tcp_response_timeout"
	outboundReasonReplyPublishFailed    = "reply_publish_failed"
)
