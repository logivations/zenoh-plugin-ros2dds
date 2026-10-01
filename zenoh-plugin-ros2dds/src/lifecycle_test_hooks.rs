// Copyright (c) 2026 Logivations
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

//! Full-binary lab hooks. This entire module is absent from production builds.
use std::{
    path::PathBuf,
    sync::atomic::{AtomicIsize, Ordering},
    time::{Duration, Instant},
};

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Command {
    InvalidateReader { service: String },
    OrphanWriter,
    RetireSubscriber { topic: String },
    FailCreation { after: usize },
    PauseCreation { after: usize },
}

static PAUSE_AT: AtomicIsize = AtomicIsize::new(-1);

pub(crate) fn control() -> Option<(PathBuf, Result<Command, String>)> {
    let path = PathBuf::from(std::env::var_os("ROS2DDS_LIFECYCLE_FAULT_FILE")?);
    let data = std::fs::read(&path).ok()?;
    let result = std::fs::remove_file(&path)
        .map_err(|e| e.to_string())
        .and_then(|_| serde_json::from_slice(&data).map_err(|e| e.to_string()));
    Some((path, result))
}

pub(crate) fn arm_pause(after: usize) {
    PAUSE_AT.store(after as isize, Ordering::SeqCst);
}

pub(crate) fn creation_checkpoint(stage: &str) -> Result<(), String> {
    if PAUSE_AT.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
        (n >= 0).then_some(n - 1)
    }) != Ok(0)
    {
        return Ok(());
    }
    let path = PathBuf::from(
        std::env::var_os("ROS2DDS_LIFECYCLE_FAULT_FILE").ok_or("No test control path")?,
    );
    std::fs::write(path.with_extension("reached"), stage).map_err(|e| e.to_string())?;
    let release = path.with_extension("release");
    let until = Instant::now() + Duration::from_secs(30);
    while !release.exists() {
        if Instant::now() >= until {
            return Err("Test barrier release timed out".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::remove_file(release).map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) fn acknowledge(path: PathBuf, result: Result<(), String>) {
    let data = serde_json::json!({"ok": result.is_ok(), "error": result.err()});
    let _ = std::fs::write(path.with_extension("ack"), data.to_string());
}
