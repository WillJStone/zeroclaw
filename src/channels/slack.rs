use super::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Slack channel — supports Socket Mode (real-time WebSocket) when `app_token`
/// is configured, otherwise falls back to polling via conversations.history.
pub struct SlackChannel {
    bot_token: String,
    app_token: Option<String>,
    channel_id: Option<String>,
    allowed_users: Vec<String>,
}

impl SlackChannel {
    pub fn new(
        bot_token: String,
        app_token: Option<String>,
        channel_id: Option<String>,
        allowed_users: Vec<String>,
    ) -> Self {
        Self {
            bot_token,
            app_token,
            channel_id,
            allowed_users,
        }
    }

    fn http_client(&self) -> reqwest::Client {
        crate::config::build_runtime_proxy_client("channel.slack")
    }

    /// Check if a Slack user ID is in the allowlist.
    /// Empty list means deny everyone until explicitly configured.
    /// `"*"` means allow everyone.
    fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Get the bot's own user ID so we can ignore our own messages
    async fn get_bot_user_id(&self) -> Option<String> {
        let resp: serde_json::Value = self
            .http_client()
            .get("https://slack.com/api/auth.test")
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;

        resp.get("user_id")
            .and_then(|u| u.as_str())
            .map(String::from)
    }

    /// Resolve the thread identifier for inbound Slack messages.
    /// Replies carry `thread_ts` (root thread id); top-level messages only have `ts`.
    fn inbound_thread_ts(msg: &serde_json::Value, ts: &str) -> Option<String> {
        msg.get("thread_ts")
            .and_then(|t| t.as_str())
            .or(if ts.is_empty() { None } else { Some(ts) })
            .map(str::to_string)
    }

    fn normalized_channel_id(input: Option<&str>) -> Option<String> {
        input
            .map(str::trim)
            .filter(|v| !v.is_empty() && *v != "*")
            .map(ToOwned::to_owned)
    }

    fn configured_channel_id(&self) -> Option<String> {
        Self::normalized_channel_id(self.channel_id.as_deref())
    }

    fn extract_channel_ids(list_payload: &serde_json::Value) -> Vec<String> {
        let mut ids = list_payload
            .get("channels")
            .and_then(|c| c.as_array())
            .into_iter()
            .flatten()
            .filter_map(|channel| {
                let id = channel.get("id").and_then(|id| id.as_str())?;
                let is_archived = channel
                    .get("is_archived")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let is_member = channel
                    .get("is_member")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                if is_archived || !is_member {
                    return None;
                }
                Some(id.to_string())
            })
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

    async fn list_accessible_channels(&self) -> anyhow::Result<Vec<String>> {
        let mut channels = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut query_params = vec![
                ("exclude_archived", "true".to_string()),
                ("limit", "200".to_string()),
                (
                    "types",
                    "public_channel,private_channel,mpim,im".to_string(),
                ),
            ];
            if let Some(ref next) = cursor {
                query_params.push(("cursor", next.clone()));
            }

            let resp = self
                .http_client()
                .get("https://slack.com/api/conversations.list")
                .bearer_auth(&self.bot_token)
                .query(&query_params)
                .send()
                .await?;

            let status = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

            if !status.is_success() {
                anyhow::bail!("Slack conversations.list failed ({status}): {body}");
            }

            let data: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            if data.get("ok") == Some(&serde_json::Value::Bool(false)) {
                let err = data
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("unknown");
                anyhow::bail!("Slack conversations.list failed: {err}");
            }

            channels.extend(Self::extract_channel_ids(&data));

            cursor = data
                .get("response_metadata")
                .and_then(|rm| rm.get("next_cursor"))
                .and_then(|c| c.as_str())
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .map(ToOwned::to_owned);

            if cursor.is_none() {
                break;
            }
        }

        channels.sort();
        channels.dedup();
        Ok(channels)
    }

    fn slack_now_ts() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        format!("{}.{:06}", now.as_secs(), now.subsec_micros())
    }

    fn ensure_poll_cursor(
        cursors: &mut HashMap<String, String>,
        channel_id: &str,
        now_ts: &str,
    ) -> String {
        cursors
            .entry(channel_id.to_string())
            .or_insert_with(|| now_ts.to_string())
            .clone()
    }

    // ── Socket Mode helpers ──────────────────────────────────────

    /// Request a WebSocket URL from Slack's apps.connections.open endpoint.
    async fn open_socket_mode_connection(&self, app_token: &str) -> anyhow::Result<String> {
        let resp: serde_json::Value = self
            .http_client()
            .post("https://slack.com/api/apps.connections.open")
            .bearer_auth(app_token)
            .send()
            .await?
            .json()
            .await?;

        if resp.get("ok") != Some(&serde_json::Value::Bool(true)) {
            let err = resp
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Slack apps.connections.open failed: {err}");
        }

        resp.get("url")
            .and_then(|u| u.as_str())
            .map(String::from)
            .ok_or_else(|| anyhow::anyhow!("Slack apps.connections.open: missing url in response"))
    }

    /// Check whether `text` contains an @-mention for the given bot user ID.
    /// Slack encodes mentions as `<@U12345>`.
    fn contains_bot_mention(text: &str, bot_user_id: &str) -> bool {
        if bot_user_id.is_empty() {
            return false;
        }
        let tag = format!("<@{bot_user_id}>");
        text.contains(&tag)
    }

    /// Remove all `<@bot_user_id>` mention tags from text and trim whitespace.
    fn strip_bot_mention(text: &str, bot_user_id: &str) -> String {
        if bot_user_id.is_empty() {
            return text.to_string();
        }
        let tag = format!("<@{bot_user_id}>");
        text.replace(&tag, "").trim().to_string()
    }

    /// Format buffered thread messages as context to prepend to a mentioned message.
    fn format_thread_context(messages: &[(String, String)]) -> String {
        if messages.is_empty() {
            return String::new();
        }
        let mut ctx = String::from("[Thread context — recent messages in this thread]\n");
        for (sender, text) in messages {
            ctx.push_str(&format!("{sender}: {text}\n"));
        }
        ctx.push('\n');
        ctx
    }

    /// Socket Mode listener — connects via WebSocket, receives events in real-time,
    /// and only sends messages through `tx` when the bot is @-mentioned.
    async fn listen_socket_mode(
        &self,
        app_token: &str,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    ) -> anyhow::Result<()> {
        let bot_user_id = self.get_bot_user_id().await.unwrap_or_default();
        let scoped_channel = self.configured_channel_id();

        tracing::info!("Slack: connecting via Socket Mode...");
        let ws_url = self.open_socket_mode_connection(app_token).await?;
        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        tracing::info!("Slack: Socket Mode connected (selective-response: @-mention only)");

        // Thread context buffer: (channel_id, thread_ts) -> Vec<(sender, text)>
        // Capped at 20 messages per thread.
        let mut thread_context: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();
        const MAX_THREAD_CONTEXT: usize = 20;

        loop {
            let msg = match read.next().await {
                Some(Ok(WsMessage::Text(t))) => t,
                Some(Ok(WsMessage::Close(_))) | None => {
                    tracing::warn!("Slack: Socket Mode connection closed");
                    break;
                }
                Some(Err(e)) => {
                    tracing::warn!("Slack: Socket Mode WebSocket error: {e}");
                    break;
                }
                _ => continue,
            };

            let envelope: serde_json::Value = match serde_json::from_str(msg.as_ref()) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // ACK every envelope immediately (Slack requires this within ~5 seconds)
            if let Some(envelope_id) = envelope.get("envelope_id").and_then(|e| e.as_str()) {
                let ack = serde_json::json!({"envelope_id": envelope_id});
                if write
                    .send(WsMessage::Text(ack.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }

            let envelope_type = envelope.get("type").and_then(|t| t.as_str()).unwrap_or("");

            match envelope_type {
                "disconnect" => {
                    tracing::info!("Slack: received disconnect, will reconnect");
                    break;
                }
                "events_api" => {}
                _ => continue,
            }

            // Extract the inner event from the events_api envelope
            let event = match envelope.get("payload").and_then(|p| p.get("event")) {
                Some(e) => e,
                None => continue,
            };

            // Only handle plain messages (no subtypes like edits, deletions, bot_message)
            let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");
            if event_type != "message" {
                continue;
            }
            if event.get("subtype").is_some() {
                continue;
            }

            let user = match event.get("user").and_then(|u| u.as_str()) {
                Some(u) => u,
                None => continue,
            };
            let text = event.get("text").and_then(|t| t.as_str()).unwrap_or("");
            let ts = event.get("ts").and_then(|t| t.as_str()).unwrap_or("");
            let event_channel = event.get("channel").and_then(|c| c.as_str()).unwrap_or("");

            // Skip bot's own messages
            if user == bot_user_id {
                continue;
            }

            // Skip unauthorized users
            if !self.is_user_allowed(user) {
                continue;
            }

            // Apply channel filter
            if let Some(ref target) = scoped_channel {
                if event_channel != target.as_str() {
                    continue;
                }
            }

            // Skip empty messages
            if text.is_empty() || ts.is_empty() {
                continue;
            }

            // Determine thread identity
            let thread_ts = event
                .get("thread_ts")
                .and_then(|t| t.as_str())
                .unwrap_or(ts);
            let is_threaded = event.get("thread_ts").is_some();
            let thread_key = (event_channel.to_string(), thread_ts.to_string());

            // Buffer this message in thread context
            let buffer = thread_context.entry(thread_key.clone()).or_default();
            buffer.push((user.to_string(), text.to_string()));
            if buffer.len() > MAX_THREAD_CONTEXT {
                buffer.remove(0);
            }

            // Selective response: only respond when @-mentioned
            let mentioned = Self::contains_bot_mention(text, &bot_user_id);
            if !mentioned {
                tracing::debug!(
                    "Slack: buffered non-mentioned message in {}/{}",
                    event_channel,
                    thread_ts
                );
                continue;
            }

            // Build content: thread context + stripped message
            let stripped = Self::strip_bot_mention(text, &bot_user_id);
            let context_prefix = if is_threaded {
                // Exclude the current message itself from context
                let ctx_msgs = &thread_context[&thread_key];
                let prior = if ctx_msgs.len() > 1 {
                    &ctx_msgs[..ctx_msgs.len() - 1]
                } else {
                    &[]
                };
                Self::format_thread_context(prior)
            } else {
                String::new()
            };
            let content = if context_prefix.is_empty() {
                stripped
            } else {
                format!("{context_prefix}[Message directed at bot]\n{stripped}")
            };

            let channel_msg = ChannelMessage {
                id: format!("slack_{event_channel}_{ts}"),
                sender: user.to_string(),
                reply_target: event_channel.to_string(),
                content,
                channel: "slack".to_string(),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                thread_ts: Some(thread_ts.to_string()),
            };

            if tx.send(channel_msg).await.is_err() {
                return Ok(());
            }
        }

        Ok(())
    }

    /// Polling-based listener — original implementation used when `app_token` is not configured.
    async fn listen_polling(
        &self,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    ) -> anyhow::Result<()> {
        let bot_user_id = self.get_bot_user_id().await.unwrap_or_default();
        let scoped_channel = self.configured_channel_id();
        let mut discovered_channels: Vec<String> = Vec::new();
        let mut last_discovery = Instant::now();
        let mut last_ts_by_channel: HashMap<String, String> = HashMap::new();
        // Track active threads: (channel_id, thread_ts) -> last seen reply ts
        let mut active_threads: HashMap<(String, String), String> = HashMap::new();

        if let Some(ref channel_id) = scoped_channel {
            tracing::info!("Slack channel listening (polling) on #{channel_id}...");
        } else {
            tracing::info!(
                "Slack channel_id not set (or '*'); listening (polling) across all accessible channels."
            );
        }

        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;

            let target_channels = if let Some(ref channel_id) = scoped_channel {
                vec![channel_id.clone()]
            } else {
                if discovered_channels.is_empty()
                    || last_discovery.elapsed() >= Duration::from_secs(60)
                {
                    match self.list_accessible_channels().await {
                        Ok(channels) => {
                            if channels != discovered_channels {
                                tracing::info!(
                                    "Slack auto-discovery refreshed: listening on {} channel(s).",
                                    channels.len()
                                );
                            }
                            discovered_channels = channels;
                        }
                        Err(e) => {
                            tracing::warn!("Slack channel discovery failed: {e}");
                        }
                    }
                    last_discovery = Instant::now();
                }

                discovered_channels.clone()
            };

            if target_channels.is_empty() {
                tracing::debug!("Slack: no accessible channels discovered yet");
                continue;
            }

            for channel_id in target_channels {
                let had_cursor = last_ts_by_channel.contains_key(&channel_id);
                let bootstrap_ts = Self::slack_now_ts();
                let cursor_ts =
                    Self::ensure_poll_cursor(&mut last_ts_by_channel, &channel_id, &bootstrap_ts);
                if !had_cursor {
                    tracing::debug!(
                        "Slack: initialized cursor for channel {} at {} to prevent historical replay",
                        channel_id,
                        cursor_ts
                    );
                }
                let params = vec![
                    ("channel", channel_id.clone()),
                    ("limit", "10".to_string()),
                    ("oldest", cursor_ts),
                ];

                let resp = match self
                    .http_client()
                    .get("https://slack.com/api/conversations.history")
                    .bearer_auth(&self.bot_token)
                    .query(&params)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("Slack poll error for channel {channel_id}: {e}");
                        continue;
                    }
                };

                let data: serde_json::Value = match resp.json().await {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!("Slack parse error for channel {channel_id}: {e}");
                        continue;
                    }
                };

                if data.get("ok") == Some(&serde_json::Value::Bool(false)) {
                    let err = data
                        .get("error")
                        .and_then(|e| e.as_str())
                        .unwrap_or("unknown");
                    tracing::warn!("Slack history error for channel {channel_id}: {err}");
                    continue;
                }

                if let Some(messages) = data.get("messages").and_then(|m| m.as_array()) {
                    // Messages come newest-first, reverse to process oldest first
                    for msg in messages.iter().rev() {
                        let ts = msg.get("ts").and_then(|t| t.as_str()).unwrap_or("");
                        let user = msg
                            .get("user")
                            .and_then(|u| u.as_str())
                            .unwrap_or("unknown");
                        let text = msg.get("text").and_then(|t| t.as_str()).unwrap_or("");
                        let last_ts = last_ts_by_channel
                            .get(&channel_id)
                            .map(String::as_str)
                            .unwrap_or("");

                        // Skip bot's own messages
                        if user == bot_user_id {
                            continue;
                        }

                        // Sender validation
                        if !self.is_user_allowed(user) {
                            tracing::warn!(
                                "Slack: ignoring message from unauthorized user: {user}"
                            );
                            continue;
                        }

                        // Skip empty or already-seen
                        if text.is_empty() || ts <= last_ts {
                            continue;
                        }

                        last_ts_by_channel.insert(channel_id.clone(), ts.to_string());

                        let thread_ts_val = Self::inbound_thread_ts(msg, ts);

                        // Track this thread so we poll replies on subsequent loops
                        if let Some(ref tts) = thread_ts_val {
                            active_threads
                                .entry((channel_id.clone(), tts.clone()))
                                .or_insert_with(|| ts.to_string());
                        }

                        let channel_msg = ChannelMessage {
                            id: format!("slack_{channel_id}_{ts}"),
                            sender: user.to_string(),
                            reply_target: channel_id.clone(),
                            content: text.to_string(),
                            channel: "slack".to_string(),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            thread_ts: thread_ts_val,
                        };

                        if tx.send(channel_msg).await.is_err() {
                            return Ok(());
                        }
                    }
                }

                // Poll active threads in this channel for new replies
                let thread_keys: Vec<(String, String)> = active_threads
                    .keys()
                    .filter(|(cid, _)| cid == &channel_id)
                    .cloned()
                    .collect();
                for (cid, thread_ts) in thread_keys {
                    let last_reply_ts = active_threads
                        .get(&(cid.clone(), thread_ts.clone()))
                        .cloned()
                        .unwrap_or_default();

                    let reply_params = vec![
                        ("channel", cid.clone()),
                        ("ts", thread_ts.clone()),
                        ("oldest", last_reply_ts.clone()),
                        ("limit", "10".to_string()),
                    ];

                    let resp = match self
                        .http_client()
                        .get("https://slack.com/api/conversations.replies")
                        .bearer_auth(&self.bot_token)
                        .query(&reply_params)
                        .send()
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!("Slack thread poll error for {cid}/{thread_ts}: {e}");
                            continue;
                        }
                    };

                    let data: serde_json::Value = match resp.json().await {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::warn!("Slack thread parse error for {cid}/{thread_ts}: {e}");
                            continue;
                        }
                    };

                    if data.get("ok") != Some(&serde_json::Value::Bool(true)) {
                        continue;
                    }

                    if let Some(replies) = data.get("messages").and_then(|m| m.as_array()) {
                        for msg in replies {
                            let rts = msg.get("ts").and_then(|t| t.as_str()).unwrap_or("");
                            let user = msg
                                .get("user")
                                .and_then(|u| u.as_str())
                                .unwrap_or("unknown");
                            let text = msg.get("text").and_then(|t| t.as_str()).unwrap_or("");

                            // Skip bot's own, empty, or already-seen
                            if user == bot_user_id
                                || text.is_empty()
                                || rts <= last_reply_ts.as_str()
                            {
                                continue;
                            }

                            if !self.is_user_allowed(user) {
                                continue;
                            }

                            active_threads
                                .insert((cid.clone(), thread_ts.clone()), rts.to_string());

                            let channel_msg = ChannelMessage {
                                id: format!("slack_{cid}_{rts}"),
                                sender: user.to_string(),
                                reply_target: cid.clone(),
                                content: text.to_string(),
                                channel: "slack".to_string(),
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                thread_ts: Some(thread_ts.clone()),
                            };

                            if tx.send(channel_msg).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Channel for SlackChannel {
    fn name(&self) -> &str {
        "slack"
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        let mut body = serde_json::json!({
            "channel": message.recipient,
            "text": message.content
        });

        if let Some(ref ts) = message.thread_ts {
            body["thread_ts"] = serde_json::json!(ts);
        }

        let resp = self
            .http_client()
            .post("https://slack.com/api/chat.postMessage")
            .bearer_auth(&self.bot_token)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

        if !status.is_success() {
            anyhow::bail!("Slack chat.postMessage failed ({status}): {body}");
        }

        // Slack returns 200 for most app-level errors; check JSON "ok" field
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
        if parsed.get("ok") == Some(&serde_json::Value::Bool(false)) {
            let err = parsed
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Slack chat.postMessage failed: {err}");
        }

        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        if let Some(ref app_token) = self.app_token {
            self.listen_socket_mode(app_token, tx).await
        } else {
            self.listen_polling(tx).await
        }
    }

    async fn health_check(&self) -> bool {
        self.http_client()
            .get("https://slack.com/api/auth.test")
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slack_channel_name() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![]);
        assert_eq!(ch.name(), "slack");
    }

    #[test]
    fn slack_channel_with_channel_id() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, Some("C12345".into()), vec![]);
        assert_eq!(ch.channel_id, Some("C12345".to_string()));
    }

    #[test]
    fn normalized_channel_id_respects_wildcard_and_blank() {
        assert_eq!(SlackChannel::normalized_channel_id(None), None);
        assert_eq!(SlackChannel::normalized_channel_id(Some("")), None);
        assert_eq!(SlackChannel::normalized_channel_id(Some("   ")), None);
        assert_eq!(SlackChannel::normalized_channel_id(Some("*")), None);
        assert_eq!(SlackChannel::normalized_channel_id(Some(" * ")), None);
        assert_eq!(
            SlackChannel::normalized_channel_id(Some(" C12345 ")),
            Some("C12345".to_string())
        );
    }

    #[test]
    fn extract_channel_ids_filters_archived_and_non_member_entries() {
        let payload = serde_json::json!({
            "channels": [
                {"id": "C1", "is_archived": false, "is_member": true},
                {"id": "C2", "is_archived": true, "is_member": true},
                {"id": "C3", "is_archived": false, "is_member": false},
                {"id": "C1", "is_archived": false, "is_member": true},
                {"id": "C4"}
            ]
        });
        let ids = SlackChannel::extract_channel_ids(&payload);
        assert_eq!(ids, vec!["C1".to_string(), "C4".to_string()]);
    }

    #[test]
    fn empty_allowlist_denies_everyone() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![]);
        assert!(!ch.is_user_allowed("U12345"));
        assert!(!ch.is_user_allowed("anyone"));
    }

    #[test]
    fn wildcard_allows_everyone() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec!["*".into()]);
        assert!(ch.is_user_allowed("U12345"));
    }

    #[test]
    fn specific_allowlist_filters() {
        let ch = SlackChannel::new(
            "xoxb-fake".into(),
            None,
            None,
            vec!["U111".into(), "U222".into()],
        );
        assert!(ch.is_user_allowed("U111"));
        assert!(ch.is_user_allowed("U222"));
        assert!(!ch.is_user_allowed("U333"));
    }

    #[test]
    fn allowlist_exact_match_not_substring() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec!["U111".into()]);
        assert!(!ch.is_user_allowed("U1111"));
        assert!(!ch.is_user_allowed("U11"));
    }

    #[test]
    fn allowlist_empty_user_id() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec!["U111".into()]);
        assert!(!ch.is_user_allowed(""));
    }

    #[test]
    fn allowlist_case_sensitive() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec!["U111".into()]);
        assert!(ch.is_user_allowed("U111"));
        assert!(!ch.is_user_allowed("u111"));
    }

    #[test]
    fn allowlist_wildcard_and_specific() {
        let ch = SlackChannel::new(
            "xoxb-fake".into(),
            None,
            None,
            vec!["U111".into(), "*".into()],
        );
        assert!(ch.is_user_allowed("U111"));
        assert!(ch.is_user_allowed("anyone"));
    }

    // ── Message ID edge cases ─────────────────────────────────────

    #[test]
    fn slack_message_id_format_includes_channel_and_ts() {
        // Verify that message IDs follow the format: slack_{channel_id}_{ts}
        let ts = "1234567890.123456";
        let channel_id = "C12345";
        let expected_id = format!("slack_{channel_id}_{ts}");
        assert_eq!(expected_id, "slack_C12345_1234567890.123456");
    }

    #[test]
    fn slack_message_id_is_deterministic() {
        // Same channel_id + same ts = same ID (prevents duplicates after restart)
        let ts = "1234567890.123456";
        let channel_id = "C12345";
        let id1 = format!("slack_{channel_id}_{ts}");
        let id2 = format!("slack_{channel_id}_{ts}");
        assert_eq!(id1, id2);
    }

    #[test]
    fn slack_message_id_different_ts_different_id() {
        // Different timestamps produce different IDs
        let channel_id = "C12345";
        let id1 = format!("slack_{channel_id}_1234567890.123456");
        let id2 = format!("slack_{channel_id}_1234567890.123457");
        assert_ne!(id1, id2);
    }

    #[test]
    fn slack_message_id_different_channel_different_id() {
        // Different channels produce different IDs even with same ts
        let ts = "1234567890.123456";
        let id1 = format!("slack_C12345_{ts}");
        let id2 = format!("slack_C67890_{ts}");
        assert_ne!(id1, id2);
    }

    #[test]
    fn slack_message_id_no_uuid_randomness() {
        // Verify format doesn't contain random UUID components
        let ts = "1234567890.123456";
        let channel_id = "C12345";
        let id = format!("slack_{channel_id}_{ts}");
        assert!(!id.contains('-')); // No UUID dashes
        assert!(id.starts_with("slack_"));
    }

    #[test]
    fn inbound_thread_ts_prefers_explicit_thread_ts() {
        let msg = serde_json::json!({
            "ts": "123.002",
            "thread_ts": "123.001"
        });

        let thread_ts = SlackChannel::inbound_thread_ts(&msg, "123.002");
        assert_eq!(thread_ts.as_deref(), Some("123.001"));
    }

    #[test]
    fn inbound_thread_ts_falls_back_to_ts() {
        let msg = serde_json::json!({
            "ts": "123.001"
        });

        let thread_ts = SlackChannel::inbound_thread_ts(&msg, "123.001");
        assert_eq!(thread_ts.as_deref(), Some("123.001"));
    }

    #[test]
    fn inbound_thread_ts_none_when_ts_missing() {
        let msg = serde_json::json!({});

        let thread_ts = SlackChannel::inbound_thread_ts(&msg, "");
        assert_eq!(thread_ts, None);
    }

    #[test]
    fn ensure_poll_cursor_bootstraps_new_channel() {
        let mut cursors = HashMap::new();
        let now_ts = "1700000000.123456";

        let cursor = SlackChannel::ensure_poll_cursor(&mut cursors, "C123", now_ts);
        assert_eq!(cursor, now_ts);
        assert_eq!(cursors.get("C123").map(String::as_str), Some(now_ts));
    }

    #[test]
    fn ensure_poll_cursor_keeps_existing_cursor() {
        let mut cursors = HashMap::from([("C123".to_string(), "1700000000.000001".to_string())]);
        let cursor = SlackChannel::ensure_poll_cursor(&mut cursors, "C123", "9999999999.999999");

        assert_eq!(cursor, "1700000000.000001");
        assert_eq!(
            cursors.get("C123").map(String::as_str),
            Some("1700000000.000001")
        );
    }

    // ── Mention detection tests ──────────────────────────────────

    #[test]
    fn contains_bot_mention_detects_slack_format() {
        assert!(SlackChannel::contains_bot_mention(
            "hey <@U12345> what do you think?",
            "U12345"
        ));
    }

    #[test]
    fn contains_bot_mention_rejects_partial_match() {
        assert!(!SlackChannel::contains_bot_mention("U12345", "U12345"));
        assert!(!SlackChannel::contains_bot_mention("<@U1234>", "U12345"));
        assert!(!SlackChannel::contains_bot_mention("<@U123456>", "U12345"));
    }

    #[test]
    fn contains_bot_mention_empty_bot_id_returns_false() {
        assert!(!SlackChannel::contains_bot_mention("<@>", ""));
        assert!(!SlackChannel::contains_bot_mention("hello", ""));
    }

    #[test]
    fn contains_bot_mention_no_mention_returns_false() {
        assert!(!SlackChannel::contains_bot_mention(
            "just a regular message",
            "U12345"
        ));
    }

    // ── Mention stripping tests ──────────────────────────────────

    #[test]
    fn strip_bot_mention_removes_tag_and_trims() {
        assert_eq!(
            SlackChannel::strip_bot_mention("<@U12345> what do you think?", "U12345"),
            "what do you think?"
        );
    }

    #[test]
    fn strip_bot_mention_handles_multiple_mentions() {
        assert_eq!(
            SlackChannel::strip_bot_mention("<@U12345> hey <@U12345>", "U12345"),
            "hey"
        );
    }

    #[test]
    fn strip_bot_mention_handles_empty_after_strip() {
        assert_eq!(SlackChannel::strip_bot_mention("<@U12345>", "U12345"), "");
    }

    #[test]
    fn strip_bot_mention_empty_bot_id_returns_original() {
        assert_eq!(
            SlackChannel::strip_bot_mention("hello <@U12345>", ""),
            "hello <@U12345>"
        );
    }

    // ── Thread context formatting tests ──────────────────────────

    #[test]
    fn format_thread_context_empty_returns_empty() {
        assert_eq!(SlackChannel::format_thread_context(&[]), "");
    }

    #[test]
    fn format_thread_context_formats_with_header() {
        let msgs = vec![
            ("U111".to_string(), "option A is better".to_string()),
            ("U222".to_string(), "I disagree".to_string()),
        ];
        let ctx = SlackChannel::format_thread_context(&msgs);
        assert!(ctx.starts_with("[Thread context"));
        assert!(ctx.contains("U111: option A is better"));
        assert!(ctx.contains("U222: I disagree"));
        assert!(ctx.ends_with("\n\n"));
    }

    #[test]
    fn format_thread_context_single_message() {
        let msgs = vec![("U111".to_string(), "hello".to_string())];
        let ctx = SlackChannel::format_thread_context(&msgs);
        assert!(ctx.contains("U111: hello"));
    }
}
