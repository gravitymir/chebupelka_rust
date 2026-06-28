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
}

#[derive(Deserialize)]
struct ChatBody {
    messages: Vec<Msg>,
}

pub async fn run(be: Backend, bind: &str) -> Result<()> {
    let state = AppState {
        be,
        stats: Stats::new(),
    };
    let router = Router::new()
        .route("/", get(index))
        .route("/api/chat", post(chat))
        .route("/api/chat/stream", post(chat_stream))
        .route("/api/stats", get(stats_handler))
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
