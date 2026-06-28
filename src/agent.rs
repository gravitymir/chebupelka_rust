//! Агентский режим: модель сама вызывает инструменты, а мы исполняем их под
//! контролем разрешений и показываем каждый шаг в браузере.
//!
//! Идея цикла взята из проекта chebupelka (минимальный Python-агент): шлём
//! модели задачу + список инструментов; пока она просит вызовы — исполняем и
//! возвращаем результаты как сообщения роли `tool`; перестала просить — это
//! финальный ответ. Сверху мы добавили своё:
//!   1) разрешения — агенту видны ТОЛЬКО разрешённые пользователем инструменты;
//!   2) наблюдаемость — каждый вызов/результат стримится в UI как событие.
//!
//! Сам HTTP-протокол llama-server живёт в backend.rs; тут — только логика агента.

use async_stream::stream;
use futures_util::Stream;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

use crate::backend::Backend;

const MAX_TURNS: usize = 8; // защита от бесконечного цикла вызовов
const MAX_RESULT_CHARS: usize = 4000; // не раздуваем контекст результатами

/// Каталог инструментов: имя + человекочитаемое описание (для галочек в UI).
pub fn catalog() -> Vec<(&'static str, &'static str)> {
    vec![
        ("read_file", "Прочитать текстовый файл"),
        ("list_dir", "Показать содержимое папки"),
        ("write_file", "Записать (создать/перезаписать) файл"),
        ("run_command", "Выполнить команду в системе"),
    ]
}

/// OpenAI-описание одного инструмента по имени (схема параметров).
fn tool_schema(name: &str) -> Option<Value> {
    let f = match name {
        "read_file" => json!({
            "name": "read_file",
            "description": "Прочитать содержимое текстового файла (путь относительно рабочей папки).",
            "parameters": {"type":"object","properties":{
                "path":{"type":"string","description":"Путь к файлу"}
            },"required":["path"]}
        }),
        "list_dir" => json!({
            "name": "list_dir",
            "description": "Показать список файлов и папок в каталоге.",
            "parameters": {"type":"object","properties":{
                "path":{"type":"string","description":"Путь к каталогу (по умолчанию текущий)"}
            },"required":["path"]}
        }),
        "write_file" => json!({
            "name": "write_file",
            "description": "Создать или перезаписать текстовый файл указанным содержимым.",
            "parameters": {"type":"object","properties":{
                "path":{"type":"string","description":"Путь к файлу"},
                "content":{"type":"string","description":"Что записать"}
            },"required":["path","content"]}
        }),
        "run_command" => json!({
            "name": "run_command",
            "description": "Выполнить команду оболочки и вернуть вывод (stdout/stderr/код выхода).",
            "parameters": {"type":"object","properties":{
                "command":{"type":"string","description":"Команда"}
            },"required":["command"]}
        }),
        _ => return None,
    };
    Some(json!({"type":"function","function": f}))
}

/// Массив `tools` только из разрешённых пользователем инструментов.
fn allowed_tools_json(allowed: &[String]) -> Value {
    let arr: Vec<Value> = allowed.iter().filter_map(|n| tool_schema(n)).collect();
    Value::Array(arr)
}

/// Событие в жизни агента — то, что видит пользователь в браузере.
pub enum AgentEvent {
    Status(String),
    Assistant(String),
    ToolCall { name: String, args: String },
    ToolResult { name: String, ok: bool, result: String },
    Denied { name: String },
    Final(String),
    Error(String),
}

impl AgentEvent {
    /// Превратить в пару (имя SSE-события, JSON-данные).
    pub fn to_sse(&self) -> (&'static str, Value) {
        match self {
            AgentEvent::Status(s) => ("status", json!({ "text": s })),
            AgentEvent::Assistant(s) => ("assistant", json!({ "text": s })),
            AgentEvent::ToolCall { name, args } => ("tool_call", json!({ "name": name, "args": args })),
            AgentEvent::ToolResult { name, ok, result } => {
                ("tool_result", json!({ "name": name, "ok": ok, "result": result }))
            }
            AgentEvent::Denied { name } => ("denied", json!({ "name": name })),
            AgentEvent::Final(s) => ("final", json!({ "text": s })),
            AgentEvent::Error(s) => ("error", json!({ "text": s })),
        }
    }
}

fn truncate(mut s: String) -> String {
    if s.chars().count() > MAX_RESULT_CHARS {
        s = s.chars().take(MAX_RESULT_CHARS).collect::<String>();
        s.push_str("\n…(вывод обрезан)");
    }
    s
}

/// Исполнить инструмент. Возвращает (успех, текст результата). Ошибки тоже
/// возвращаются текстом — модель должна их видеть и реагировать.
async fn execute_tool(name: &str, args: &str) -> (bool, String) {
    let v: Value = match serde_json::from_str(args) {
        Ok(v) => v,
        Err(e) => return (false, format!("не разобрал аргументы как JSON: {e}")),
    };
    match name {
        "read_file" => {
            let path = v["path"].as_str().unwrap_or("");
            if path.is_empty() {
                return (false, "не указан path".into());
            }
            match tokio::fs::read_to_string(path).await {
                Ok(c) => (true, truncate(c)),
                Err(e) => (false, format!("ошибка чтения «{path}»: {e}")),
            }
        }
        "list_dir" => {
            let mut path = v["path"].as_str().unwrap_or(".");
            if path.is_empty() {
                path = ".";
            }
            match tokio::fs::read_dir(path).await {
                Ok(mut rd) => {
                    let mut items = Vec::new();
                    loop {
                        match rd.next_entry().await {
                            Ok(Some(e)) => {
                                let is_dir =
                                    e.file_type().await.map(|t| t.is_dir()).unwrap_or(false);
                                let n = e.file_name().to_string_lossy().to_string();
                                items.push(if is_dir { format!("{n}/") } else { n });
                            }
                            Ok(None) => break,
                            Err(e) => return (false, format!("ошибка чтения каталога: {e}")),
                        }
                    }
                    items.sort();
                    let body = if items.is_empty() {
                        "(пусто)".to_string()
                    } else {
                        items.join("\n")
                    };
                    (true, truncate(body))
                }
                Err(e) => (false, format!("ошибка открытия каталога «{path}»: {e}")),
            }
        }
        "write_file" => {
            let path = v["path"].as_str().unwrap_or("");
            let content = v["content"].as_str().unwrap_or("");
            if path.is_empty() {
                return (false, "не указан path".into());
            }
            match tokio::fs::write(path, content).await {
                Ok(_) => (true, format!("записано {} байт в «{path}»", content.len())),
                Err(e) => (false, format!("ошибка записи «{path}»: {e}")),
            }
        }
        "run_command" => {
            let command = v["command"].as_str().unwrap_or("");
            if command.is_empty() {
                return (false, "не указан command".into());
            }
            run_command(command).await
        }
        other => (false, format!("неизвестный инструмент: {other}")),
    }
}

/// Выполнить команду через системную оболочку (cmd на Windows, sh иначе),
/// с таймаутом 60 с. stderr/stdout/код выхода — всё в результат.
async fn run_command(command: &str) -> (bool, String) {
    use tokio::process::Command;

    #[cfg(windows)]
    let mut cmd = {
        let mut c = Command::new("cmd");
        c.arg("/C").arg(command);
        c
    };
    #[cfg(not(windows))]
    let mut cmd = {
        let mut c = Command::new("sh");
        c.arg("-c").arg(command);
        c
    };

    match timeout(Duration::from_secs(60), cmd.output()).await {
        Ok(Ok(out)) => {
            let mut s = format!("код выхода: {}\n", out.status.code().unwrap_or(-1));
            s.push_str(&String::from_utf8_lossy(&out.stdout));
            if !out.stderr.is_empty() {
                s.push_str("\nSTDERR:\n");
                s.push_str(&String::from_utf8_lossy(&out.stderr));
            }
            (out.status.success(), truncate(s))
        }
        Ok(Err(e)) => (false, format!("не удалось запустить команду: {e}")),
        Err(_) => (false, "команда не уложилась в таймаут 60 c".into()),
    }
}

/// Запустить агента: возвращает поток событий (для стрима в браузер).
/// `allowed` — имена разрешённых инструментов; агенту предъявляются только они.
pub fn run(be: Backend, task: String, allowed: Vec<String>) -> impl Stream<Item = AgentEvent> {
    stream! {
        let tools = allowed_tools_json(&allowed);
        let tools_line = if allowed.is_empty() {
            "нет (работай без инструментов, отвечай из своих знаний)".to_string()
        } else {
            allowed.join(", ")
        };
        let system = format!(
            "Ты — полезный агент, работающий на компьютере пользователя. \
             Решай задачу пошагово, при необходимости вызывая инструменты. \
             Разрешённые инструменты: {tools_line}. Рабочая папка — текущая. \
             Когда задача решена, дай краткий финальный ответ БЕЗ вызова инструментов. \
             Отвечай по-русски."
        );

        let mut messages: Vec<Value> = vec![
            json!({ "role": "system", "content": system }),
            json!({ "role": "user", "content": task }),
        ];

        for turn in 1..=MAX_TURNS {
            yield AgentEvent::Status(format!("ход {turn}"));

            let at = match be.chat_tools(&messages, &tools).await {
                Ok(at) => at,
                Err(e) => {
                    yield AgentEvent::Error(e.to_string());
                    return;
                }
            };

            // ответ ассистента кладём в историю «как есть» (с tool_calls и их id)
            messages.push(at.message.clone());

            if !at.content.trim().is_empty() {
                yield AgentEvent::Assistant(at.content.clone());
            }

            if at.tool_calls.is_empty() {
                yield AgentEvent::Final(at.content);
                return;
            }

            for tc in &at.tool_calls {
                yield AgentEvent::ToolCall { name: tc.name.clone(), args: tc.arguments.clone() };

                let result = if !allowed.contains(&tc.name) {
                    // подстраховка: модель не должна знать о неразрешённых, но мало ли
                    yield AgentEvent::Denied { name: tc.name.clone() };
                    format!("❌ Инструмент «{}» не разрешён пользователем.", tc.name)
                } else {
                    let (ok, text) = execute_tool(&tc.name, &tc.arguments).await;
                    yield AgentEvent::ToolResult { name: tc.name.clone(), ok, result: text.clone() };
                    text
                };

                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                }));
            }
        }

        yield AgentEvent::Error(format!("достигнут лимит шагов ({MAX_TURNS}) — агент остановлен"));
    }
}
