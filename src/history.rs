//! Сохранение истории диалогов на диск. Каждый диалог — отдельный JSON-файл в
//! `data/conversations/`. Без БД, нарочно просто.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const DIR: &str = "data/conversations";

/// Краткая карточка диалога для списка.
#[derive(Serialize)]
pub struct Meta {
    pub id: String,
    pub title: String,
    pub ts: u64,
    pub count: usize,
}

/// Полный диалог (как лежит на диске).
#[derive(Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
    pub title: String,
    pub ts: u64,
    /// массив сообщений [{role, content}, ...]
    pub messages: Value,
}

fn dir() -> PathBuf {
    PathBuf::from(DIR)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Путь к файлу диалога с защитой от выхода за пределы папки.
fn path_for(id: &str) -> Result<PathBuf> {
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(anyhow!("некорректный id диалога"));
    }
    Ok(dir().join(format!("{id}.json")))
}

/// Заголовок диалога — первое сообщение пользователя, обрезанное.
fn title_from(messages: &Value) -> String {
    if let Some(arr) = messages.as_array() {
        for m in arr {
            if m["role"] == "user" {
                let t: String = m["content"].as_str().unwrap_or("").chars().take(60).collect();
                if !t.trim().is_empty() {
                    return t;
                }
            }
        }
    }
    "(без названия)".into()
}

/// Сохранить (создать или обновить) диалог. Возвращает его id.
pub fn save(id: Option<String>, messages: Value) -> Result<String> {
    std::fs::create_dir_all(dir())?;
    let id = match id {
        Some(s) if !s.is_empty() => s,
        _ => format!("c{}", now_ms()),
    };
    let conv = Conversation {
        id: id.clone(),
        title: title_from(&messages),
        ts: now_ms(),
        messages,
    };
    std::fs::write(path_for(&id)?, serde_json::to_vec_pretty(&conv)?)?;
    Ok(id)
}

/// Список диалогов, новые сверху.
pub fn list() -> Result<Vec<Meta>> {
    std::fs::create_dir_all(dir())?;
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir())? {
        let entry = entry?;
        if entry.path().extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if let Ok(bytes) = std::fs::read(entry.path()) {
            if let Ok(conv) = serde_json::from_slice::<Conversation>(&bytes) {
                let count = conv.messages.as_array().map(|a| a.len()).unwrap_or(0);
                out.push(Meta {
                    id: conv.id,
                    title: conv.title,
                    ts: conv.ts,
                    count,
                });
            }
        }
    }
    out.sort_by(|a, b| b.ts.cmp(&a.ts));
    Ok(out)
}

/// Загрузить один диалог целиком.
pub fn load(id: &str) -> Result<Conversation> {
    let bytes = std::fs::read(path_for(id)?).map_err(|e| anyhow!("не найден диалог «{id}»: {e}"))?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Удалить диалог.
pub fn delete(id: &str) -> Result<()> {
    let p = path_for(id)?;
    if p.exists() {
        std::fs::remove_file(p)?;
    }
    Ok(())
}
