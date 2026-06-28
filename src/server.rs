//! Веб-сервер на axum: страница чата + JSON/SSE эндпоинты + статистика.

use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        Html, IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::convert::Infallible;

use crate::backend::{Backend, Msg};
use crate::stats::Stats;

#[derive(Clone)]
struct AppState {
    be: Backend,
    stats: Stats,
    /// реестр ожидающих подтверждений для агента (режим «спрашивать»)
    approvals: crate::agent::Approvals,
}

#[derive(Deserialize)]
struct ChatBody {
    messages: Vec<Msg>,
}

#[derive(Deserialize)]
struct AgentBody {
    task: String,
    /// имена инструментов, которые пользователь разрешил агенту
    #[serde(default)]
    allowed: Vec<String>,
    /// "auto" — выполнять сразу; иначе (в т.ч. пусто) — спрашивать подтверждение
    #[serde(default)]
    mode: String,
}

#[derive(Deserialize)]
struct ApproveBody {
    id: String,
    approved: bool,
}

pub async fn run(be: Backend, bind: &str) -> Result<()> {
    let state = AppState {
        be,
        stats: Stats::new(),
        approvals: crate::agent::Approvals::new(),
    };
    let router = Router::new()
        .route("/", get(index))
        .route("/api/chat", post(chat))
        .route("/api/chat/stream", post(chat_stream))
        .route("/api/stats", get(stats_handler))
        .route("/api/agent/tools", get(agent_tools))
        .route("/api/agent/stream", post(agent_stream))
        .route("/api/agent/approve", post(agent_approve))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("llamadeck слушает http://{bind}  (Ctrl+C для выхода)");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Ждём Ctrl+C — тогда axum завершится штатно, а вызывающий код успеет погасить движок.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    println!("\nОстанавливаюсь...");
}

/// Список доступных инструментов агента (имя + описание) — для галочек в UI.
async fn agent_tools() -> impl IntoResponse {
    let items: Vec<_> = crate::agent::catalog()
        .into_iter()
        .map(|(name, desc)| serde_json::json!({ "name": name, "desc": desc }))
        .collect();
    Json(items)
}

/// Запуск агента: стримим события (вызовы инструментов, результаты, финал) по SSE.
async fn agent_stream(State(app): State<AppState>, Json(body): Json<AgentBody>) -> impl IntoResponse {
    let be = app.be.clone();
    let mode = crate::agent::PermMode::parse(&body.mode);
    let approvals = app.approvals.clone();
    let stream = async_stream::stream! {
        let events = crate::agent::run(be, body.task, body.allowed, mode, approvals);
        futures_util::pin_mut!(events);
        while let Some(ev) = events.next().await {
            let (kind, data) = ev.to_sse();
            yield Ok::<Event, Infallible>(Event::default().event(kind).data(data.to_string()));
        }
        yield Ok(Event::default().event("end").data("end"));
    };
    Sse::new(stream)
}

/// Подтверждение/отклонение конкретного вызова инструмента (режим «спрашивать»).
async fn agent_approve(State(app): State<AppState>, Json(b): Json<ApproveBody>) -> impl IntoResponse {
    let found = app.approvals.resolve(&b.id, b.approved);
    Json(serde_json::json!({ "ok": found }))
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../assets/index.html"))
}

async fn stats_handler(State(app): State<AppState>) -> impl IntoResponse {
    Json(app.stats.snapshot())
}

async fn chat(State(app): State<AppState>, Json(body): Json<ChatBody>) -> impl IntoResponse {
    match app.be.complete(&body.messages).await {
        Ok(r) => {
            app.stats.record(r.tokens as u64, r.ms as u64);
            Json(serde_json::json!({
                "reply": r.reply,
                "tokens": r.tokens,
                "ms": r.ms,
                "tok_per_s": r.tok_per_s(),
            }))
            .into_response()
        }
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn chat_stream(State(app): State<AppState>, Json(body): Json<ChatBody>) -> impl IntoResponse {
    let be = app.be.clone();
    let stats = app.stats.clone();
    let stream = async_stream::stream! {
        let t = std::time::Instant::now();
        let mut count: u64 = 0;
        let inner = be.stream(body.messages);
        futures_util::pin_mut!(inner);
        while let Some(item) = inner.next().await {
            match item {
                Ok(tok) => {
                    count += 1;
                    yield Ok::<Event, Infallible>(Event::default().data(tok));
                }
                Err(e) => {
                    yield Ok(Event::default().event("error").data(e.to_string()));
                }
            }
        }
        let ms = t.elapsed().as_millis() as u64;
        stats.record(count, ms);
        yield Ok(Event::default().event("done").data(count.to_string()));
    };
    Sse::new(stream)
}
