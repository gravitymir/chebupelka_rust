//! Слой инференса. Сейчас он говорит с llama-server по OpenAI-совместимому
//! HTTP API. Если позже захочешь "один бинарник напрямую" — заменить нужно
//! ТОЛЬКО этот файл (на FFI через крейт llama-cpp-2), остальное не тронется.

use anyhow::{anyhow, Result};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Clone, Serialize, Deserialize)]
pub struct Msg {
    pub role: String,
    pub content: String,
}

#[derive(Clone)]
pub struct Backend {
    upstream: String,
    model: String,
    client: reqwest::Client,
}

#[derive(Serialize)]
struct ChatReq<'a> {
    model: &'a str,
    messages: &'a [Msg],
    stream: bool,
    temperature: f32,
}

#[derive(Deserialize)]
struct Resp {
    choices: Vec<Choice>,
    usage: Option<Usage>,
}
#[derive(Deserialize)]
struct Choice {
    message: ContentHolder,
}
#[derive(Deserialize)]
struct ContentHolder {
    content: String,
}
#[derive(Deserialize)]
struct Usage {
    completion_tokens: Option<u32>,
}

pub struct CompleteResult {
    pub reply: String,
    pub tokens: u32,
    pub ms: u128,
}
impl CompleteResult {
    pub fn tok_per_s(&self) -> f64 {
        if self.ms == 0 {
            0.0
        } else {
            self.tokens as f64 / (self.ms as f64 / 1000.0)
        }
    }
}

/// Один вызов инструмента, который запросила модель.
#[derive(Clone)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// аргументы — JSON-строка (как их отдаёт OpenAI-совместимый API)
    pub arguments: String,
}

/// Ход ассистента в агентском режиме: текст и/или запрошенные вызовы инструментов.
pub struct AssistantTurn {
    /// сырое сообщение ассистента — его нужно добавить обратно в историю «как есть»,
    /// чтобы последующие сообщения роли `tool` корректно ссылались на tool_call_id
    pub message: serde_json::Value,
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
}

impl Backend {
    pub fn new(upstream: String, model: String) -> Self {
        Self {
            upstream,
            model,
            client: reqwest::Client::new(),
        }
    }

    fn url(&self) -> String {
        format!("{}/v1/chat/completions", self.upstream.trim_end_matches('/'))
    }

    /// Один запрос -> один полный ответ.
    pub async fn complete(&self, messages: &[Msg]) -> Result<CompleteResult> {
        let body = ChatReq {
            model: &self.model,
            messages,
            stream: false,
            temperature: 0.7,
        };
        let t = Instant::now();
        let resp = self
            .client
            .post(self.url())
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("не достучался до llama-server ({}): {e}", self.upstream))?
            .error_for_status()
            .map_err(|e| anyhow!("llama-server вернул ошибку: {e}"))?;
        let parsed: Resp = resp.json().await?;
        let ms = t.elapsed().as_millis();
        let reply = parsed
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .unwrap_or_default();
        let tokens = parsed.usage.and_then(|u| u.completion_tokens).unwrap_or(0);
        Ok(CompleteResult { reply, tokens, ms })
    }

    /// Запрос с инструментами (агентский режим). Не потоковый: возвращает один
    /// ход ассистента — его текст и/или список вызовов инструментов.
    pub async fn chat_tools(
        &self,
        messages: &[serde_json::Value],
        tools: &serde_json::Value,
    ) -> Result<AssistantTurn> {
        let body = serde_json::json!({
            "model": self.model,
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": 0.2,
            "max_tokens": 1024,
        });
        let resp = self
            .client
            .post(self.url())
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("не достучался до llama-server ({}): {e}", self.upstream))?
            .error_for_status()
            .map_err(|e| anyhow!("llama-server вернул ошибку: {e}"))?;
        let v: serde_json::Value = resp.json().await?;
        let msg = v["choices"][0]["message"].clone();
        if msg.is_null() {
            return Err(anyhow!("пустой ответ от llama-server (нет choices[0].message)"));
        }
        let content = msg["content"].as_str().unwrap_or("").to_string();
        let mut tool_calls = Vec::new();
        if let Some(arr) = msg["tool_calls"].as_array() {
            for tc in arr {
                tool_calls.push(ToolCall {
                    id: tc["id"].as_str().unwrap_or("").to_string(),
                    name: tc["function"]["name"].as_str().unwrap_or("").to_string(),
                    arguments: tc["function"]["arguments"]
                        .as_str()
                        .unwrap_or("{}")
                        .to_string(),
                });
            }
        }
        Ok(AssistantTurn {
            message: msg,
            content,
            tool_calls,
        })
    }

    /// Потоковый ответ: отдаёт кусочки текста по мере генерации.
    pub fn stream(&self, messages: Vec<Msg>) -> impl Stream<Item = Result<String>> {
        let url = self.url();
        let model = self.model.clone();
        let client = self.client.clone();
        async_stream::try_stream! {
            let body = serde_json::json!({
                "model": model,
                "messages": messages,
                "stream": true,
                "temperature": 0.7,
            });
            let resp = client
                .post(url)
                .json(&body)
                .send()
                .await?
                .error_for_status()?;

            let mut bytes = resp.bytes_stream();
            let mut buf = String::new();
            while let Some(chunk) = bytes.next().await {
                let chunk = chunk?;
                buf.push_str(&String::from_utf8_lossy(&chunk));
                // SSE: события разделены переводами строк, полезное в строках "data: ..."
                while let Some(pos) = buf.find('\n') {
                    let line: String = buf[..pos].trim().to_string();
                    buf.drain(..=pos);
                    let Some(data) = line.strip_prefix("data:") else { continue };
                    let data = data.trim();
                    if data == "[DONE]" || data.is_empty() {
                        continue;
                    }
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                        if let Some(tok) = v["choices"][0]["delta"]["content"].as_str() {
                            if !tok.is_empty() {
                                yield tok.to_string();
                            }
                        }
                    }
                }
            }
        }
    }
}
