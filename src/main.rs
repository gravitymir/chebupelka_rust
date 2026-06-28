mod agent;
mod backend;
mod engine;
mod history;
mod server;
mod stats;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::io::Write;

use backend::{Backend, Msg};
use engine::{Engine, EngineOpts};

#[derive(Parser)]
#[command(name = "llamadeck", version, about = "Rust-обёртка над llama.cpp (llama-server): веб-чат, CLI, статистика")]
struct Cli {
    /// URL уже запущенного llama-server (используется, если НЕ задан --model-path)
    #[arg(long, default_value = "http://127.0.0.1:8080", global = true)]
    upstream: String,

    /// Имя модели (поле model в запросе; llama-server обычно его игнорирует)
    #[arg(long, default_value = "local-model", global = true)]
    model: String,

    /// Путь к .gguf. Если задан — llamadeck сам поднимет llama-server (один терминал)
    /// и будет говорить с ним; --upstream при этом игнорируется.
    #[arg(long, global = true)]
    model_path: Option<String>,

    /// Бинарь llama-server для авто-запуска (имя в PATH или путь к файлу)
    #[arg(long, default_value = "llama-server", global = true)]
    llama_bin: String,

    /// Порт, на котором поднимать свой llama-server (авто-запуск)
    #[arg(long, default_value_t = 8080, global = true)]
    engine_port: u16,

    /// Размер контекста (-c) для авто-запуска
    #[arg(long, default_value_t = 4096, global = true)]
    ctx: u32,

    /// Сколько слоёв выгрузить на GPU (-ngl) при авто-запуске.
    /// На T2000 (4 ГБ) начинай с малого; 0 — целиком на CPU (надёжно).
    #[arg(long, default_value_t = 0, global = true)]
    ngl: u32,

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

    // Если задан путь к модели — поднимаем собственный llama-server и говорим с ним.
    // Иначе подключаемся к уже запущенному по --upstream (старое поведение).
    let mut engine: Option<Engine> = None;
    let upstream = if let Some(model_path) = &cli.model_path {
        let host = "127.0.0.1".to_string();
        let opts = EngineOpts {
            bin: cli.llama_bin.clone(),
            model_path: model_path.clone(),
            host: host.clone(),
            port: cli.engine_port,
            ctx: cli.ctx,
            ngl: cli.ngl,
        };
        engine = Some(Engine::launch(&opts).await?);
        format!("http://{host}:{}", cli.engine_port)
    } else {
        cli.upstream.clone()
    };

    let be = Backend::new(upstream, cli.model.clone());

    let res = match cli.cmd {
        Cmd::Serve { bind } => server::run(be, &bind).await,
        Cmd::Chat => repl(be).await,
    };

    // Гасим движок, который подняли сами (на случай, если kill_on_drop не сработает).
    if let Some(mut e) = engine {
        e.shutdown().await;
    }
    res
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
