use anyhow::{Context, Result};
use std::{fs::File, path::Path};
use tempfile::NamedTempFile;

pub fn load(path: &Path) -> Result<Option<tailer::Position>> {
    match File::open(path) {
        Ok(file) => serde_json::from_reader(file)
            .map(Some)
            .with_context(|| format!("failed to deserialize Position from {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("cannot open {}", path.display())),
    }
}

pub fn save(path: &Path, pos: &tailer::Position) -> Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = NamedTempFile::new_in(dir).with_context(|| "cannot create temp file")?;
    serde_json::to_writer(&tmp, &pos)
        .with_context(|| format!("failed to serialize Position to {}", path.display()))?;
    tmp.into_temp_path()
        .persist(path)
        .with_context(|| format!("cannot persist temp file at location {}", path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tailer::Position;

    #[test]
    fn save_then_load_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replica_cmds.json");
        let pos = Position {
            path: "/data/replica_cmds/123".into(),
            line_number: 42,
            offset: 9000,
        };

        save(&path, &pos).unwrap();
        let loaded = load(&path)
            .unwrap()
            .expect("checkpoint should exist after save");

        assert_eq!(loaded.path, pos.path);
        assert_eq!(loaded.line_number, pos.line_number);
        assert_eq!(loaded.offset, pos.offset);
    }

    #[test]
    fn load_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does_not_exist.json");
        assert!(load(&path).unwrap().is_none());
    }
}
