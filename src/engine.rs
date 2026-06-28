//! Управление дочерним процессом llama-server: запуск + health-check + остановка.
//!
//! Этот модуль существует ради удобства «один терминал»: вместо того чтобы
//! отдельно крутить llama-server, llamadeck поднимает его сам и гасит при выходе.
//! Сам инференс по-прежнему идёт через HTTP (см. backend.rs). При будущем
//! переезде на FFI (`llama-cpp-2`, один бинарник) этот модуль станет не нужен и
//! будет удалён целиком — отдельного llama-server тогда не будет.

use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};

/// Параметры запуска движка.
pub struct EngineOpts {
    /// путь к бинарю llama-server (или просто имя, если он в PATH)
    pub bin: String,
    /// путь к файлу модели .gguf
    pub model_path: String,
    /// адрес, который слушает движок (обычно 127.0.0.1)
    pub host: String,
    /// порт движка
    pub port: u16,
    /// размер контекста (-c)
    pub ctx: u32,
    /// сколько слоёв выгрузить на GPU (-ngl)
    pub ngl: u32,
}

/// Запущенный нами llama-server. Благодаря `kill_on_drop` он умирает вместе с
/// llamadeck, даже если мы забудем позвать `shutdown()`.
pub struct Engine {
    child: Child,
}

impl Engine {
    /// Поднять llama-server и дождаться готовности (`/health` → 200).
    pub async fn launch(opts: &EngineOpts) -> Result<Self> {
        let mut cmd = Command::new(&opts.bin);
        cmd.arg("-m")
            .arg(&opts.model_path)
            .arg("--host")
            .arg(&opts.host)
            .arg("--port")
            .arg(opts.port.to_string())
            .arg("-c")
            .arg(opts.ctx.to_string())
            .arg("-ngl")
            .arg(opts.ngl.to_string())
            // --jinja включает родной шаблон модели и разбор tool_calls —
            // без него не работает агентский режим (вызов инструментов).
            .arg("--jinja")
            .kill_on_drop(true);

        println!(
            "Запускаю llama-server: {} -m {} --port {} -c {} -ngl {}",
            opts.bin, opts.model_path, opts.port, opts.ctx, opts.ngl
        );

        let child = cmd.spawn().map_err(|e| {
            anyhow!(
                "не удалось запустить llama-server (bin: «{}»). \
                 Проверь, что файл существует и доступен (или лежит в PATH). Причина: {e}",
                opts.bin
            )
        })?;

        let mut engine = Engine { child };
        engine.wait_healthy(&opts.host, opts.port).await?;
        Ok(engine)
    }

    /// Опрашивает `/health`, пока движок не ответит 200 (или не упадёт раньше).
    async fn wait_healthy(&mut self, host: &str, port: u16) -> Result<()> {
        let url = format!("http://{host}:{port}/health");
        let client = reqwest::Client::new();
        // загрузка крупной модели (десятки ГБ) может занять заметное время
        let deadline = Duration::from_secs(300);
        let started = Instant::now();

        println!("Жду готовности llama-server (загрузка модели может занять минуту-другую)...");
        loop {
            // если процесс уже завершился — нет смысла ждать health, сообщим причину
            if let Some(status) = self.child.try_wait()? {
                return Err(anyhow!(
                    "llama-server завершился до готовности (код {status}). \
                     Частые причины: не хватило VRAM/RAM (попробуй меньше -ngl или меньший -c) \
                     либо неверный путь к модели."
                ));
            }

            if started.elapsed() > deadline {
                return Err(anyhow!(
                    "llama-server не стал готов за {} с — что-то пошло не так при загрузке модели",
                    deadline.as_secs()
                ));
            }

            if let Ok(r) = client
                .get(&url)
                .timeout(Duration::from_secs(3))
                .send()
                .await
            {
                if r.status().is_success() {
                    println!("llama-server готов: http://{host}:{port}");
                    return Ok(());
                }
            }

            sleep(Duration::from_millis(500)).await;
        }
    }

    /// Аккуратно остановить движок.
    pub async fn shutdown(&mut self) {
        let _ = self.child.kill().await;
    }
}
