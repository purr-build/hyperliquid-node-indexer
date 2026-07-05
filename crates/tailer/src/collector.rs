use std::path::PathBuf;

use walkdir::WalkDir;

fn collect_files(dir: &PathBuf) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(dir) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            files.push(path.to_path_buf());
        }
    }

    Ok(files)
}

pub struct FilesCollector {
    files: Vec<PathBuf>,
    cursor: usize,
}

impl FilesCollector {
    pub fn new<F>(dir: PathBuf, compare: F) -> std::io::Result<Self>
    where
        F: FnMut(&PathBuf, &PathBuf) -> std::cmp::Ordering,
    {
        let mut files = collect_files(&dir)?;
        files.sort_by(compare);
        Ok(Self { files, cursor: 0 })
    }

    pub fn seek_to(&mut self, path: &PathBuf) {
        self.cursor = self.files.iter().position(|f| f == path).unwrap_or(0);
    }
}

impl Iterator for FilesCollector {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.files.len() {
            None
        } else {
            let file = self.files[self.cursor].clone();
            self.cursor += 1;
            Some(file)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::collector::FilesCollector;

    fn make_files(root: &Path, rel_paths: &[&str]) {
        for rel in rel_paths {
            let path = root.join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"").unwrap();
        }
    }

    #[test]
    fn collects_files_sorted() {
        let dir = tempfile::tempdir().unwrap();
        make_files(
            dir.path(),
            &[
                "replica_cmds/2026-05-30T08:55:03Z/20260531/1016100000",
                "replica_cmds/2026-05-30T08:55:03Z/20260531/1016150000",
                "replica_cmds/2026-05-30T08:55:03Z/20260531/1016230000",
                "replica_cmds/2026-05-29T08:55:03Z/20260530/1016000000",
            ],
        );

        let collector = FilesCollector::new(dir.path().to_path_buf(), |a, b| a.cmp(b)).unwrap();

        let got: Vec<_> = collector
            .map(|p| p.strip_prefix(dir.path()).unwrap().to_path_buf())
            .collect();

        assert_eq!(
            got,
            vec![
                PathBuf::from("replica_cmds/2026-05-29T08:55:03Z/20260530/1016000000"),
                PathBuf::from("replica_cmds/2026-05-30T08:55:03Z/20260531/1016100000"),
                PathBuf::from("replica_cmds/2026-05-30T08:55:03Z/20260531/1016150000"),
                PathBuf::from("replica_cmds/2026-05-30T08:55:03Z/20260531/1016230000"),
            ]
        );
    }
}
