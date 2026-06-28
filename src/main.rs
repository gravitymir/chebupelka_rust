mod backend;
mod server;
mod stats;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::io::Write;

use backend::{Backend, Msg};

#[derive(Parser)]
#[command(name = "llamadeck", version, about = "Rust-обёртка над llama.cpp (llama-server): веб-чат, CLI, статистика")]
struct Cli {
    /// URL уже запущенного llama-server
    #[arg(long, default_value = "http://127.0.0.1:8080", global = true)]
    upstream: String,

    /// Имя модели (поле model в запросе; llama-server обычно его игнорирует)
    #[arg(long, default_value = "local-model", global = true)]
    model: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Поднять веб-сервер: чат в браузере + статистика
    Serve {
        /// Адрес, который слушаем
        #[arg(long, default_value = "127.0.0.1:3000")]
        bind: String,
    },
    /// Чат прямо в терминале
    Chat,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let be = Backend::new(cli.upstream.clone(), cli.model.clone());

    match cli.cmd {
        Cmd::Serve { bind } => server::run(be, &bind).await?,
        Cmd::Chat => repl(be).await?,
    }
    Ok(())
}

async fn repl(be: Backend) -> Result<()> {
    println!("llamadeck CLI — пиши сообщение, /exit для выхода.\n");
    let mut history: Vec<Msg> = Vec::new();
    let stdin = std::io::stdin();

    loop {
        print!("> ");
        std::io::stdout().flush().ok();

        let mut line = String::new();
        if stdin.read_line(&mut line)? == 0 {
            break; // EOF
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "/exit" {
            break;
        }

        history.push(Msg {
            role: "user".into(),
            content: line.into(),
        });

        match be.complete(&history).await {
            Ok(r) => {
                println!(
                    "\n{}\n[{} ток · {} мс · {:.1} ток/с]\n",
                    r.reply.trim(),
                    r.tokens,
                    r.ms,
                    r.tok_per_s()
                );
                history.push(Msg {
                    role: "assistant".into(),
                    content: r.reply,
                });
            }
            Err(e) => {
                eprintln!("\nОшибка: {e}\n");
                history.pop(); // убрать неотвеченный вопрос из истории
            }
        }
    }
    Ok(())
}
