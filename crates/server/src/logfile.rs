//! The log file, rolled by size so that it does not grow without limit.
//!
//! Once the file reaches `logfile-max-size`, it is renamed `rexyra.1.log`,
//! the one before that becomes `rexyra.2.log`, and so on up to
//! `logfile-max-files`; the oldest is deleted and a new `rexyra.log` started.
//! The number goes before the extension so the old files still open with
//! whatever program opens `.log` files.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub struct RotatingLog {
    path: PathBuf,
    file: File,
    /// Bytes in the current file.
    size: u64,
    /// The size at which the file is rolled; zero never rolls it.
    max_size: u64,
    /// How many earlier files are kept beside the current one.
    max_files: u32,
}

impl RotatingLog {
    /// Opens `path` to add to it, as the log of a new run.
    pub fn open(path: impl Into<PathBuf>, max_size: u64, max_files: u32) -> io::Result<Self> {
        let path = path.into();
        let file = append(&path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            path,
            file,
            size,
            max_size,
            max_files,
        })
    }

    /// Sets a new run apart from the one before with a blank line, or, if
    /// the file is full, by starting the next file.
    pub fn begin_run(&mut self) -> io::Result<()> {
        if self.size == 0 {
            Ok(())
        } else if self.full(1) {
            self.roll();
            Ok(())
        } else {
            self.write_all(b"\n")
        }
    }

    /// Whether `more` bytes would take the file past its size.
    fn full(&self, more: usize) -> bool {
        self.max_size > 0 && self.size > 0 && self.size.saturating_add(more as u64) > self.max_size
    }

    /// The name of the `n`th earlier file: `rexyra.log` gives `rexyra.n.log`.
    pub fn earlier(&self, n: u32) -> PathBuf {
        earlier(&self.path, n)
    }

    /// Moves the current file aside and starts a new one. If the file cannot
    /// be moved, because another program has it open without allowing that,
    /// writing carries on in it and the next try is after another
    /// `max_size` bytes, so a log is never lost to a failed roll.
    fn roll(&mut self) {
        self.size = 0;
        if self.max_files == 0 {
            // A handle opened to append cannot cut the file short on
            // Windows; one opened to write can.
            let emptied = OpenOptions::new()
                .write(true)
                .open(&self.path)
                .and_then(|file| file.set_len(0));
            if let Err(e) = emptied {
                self.note(format_args!("could not empty the log file: {e}"));
            }
            return;
        }
        let _ = fs::remove_file(self.earlier(self.max_files));
        for n in (1..self.max_files).rev() {
            let from = self.earlier(n);
            if from.exists() {
                let _ = fs::rename(&from, self.earlier(n + 1));
            }
        }
        // The file this process has open can be renamed, as it is opened
        // allowing that; another program's handle may not allow it.
        if let Err(e) = fs::rename(&self.path, self.earlier(1)) {
            let max_size = self.max_size;
            self.note(format_args!(
                "could not roll the log file: {e}; trying again after another {max_size} bytes"
            ));
            return;
        }
        match append(&self.path) {
            Ok(file) => self.file = file,
            // The renamed file is still open, so the log goes on there
            // rather than nowhere.
            Err(e) => self.note(format_args!("could not start a new log file: {e}")),
        }
    }

    /// A line about the log file itself, in the log.
    fn note(&mut self, what: std::fmt::Arguments<'_>) {
        let _ = writeln!(self.file, "{what} ('{}')", self.path.display());
    }
}

impl Write for RotatingLog {
    /// The log writes each line in one call, so a line is never split
    /// between two files.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.full(buf.len()) {
            self.roll();
        }
        let written = self.file.write(buf)?;
        self.size += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn earlier(path: &Path, n: u32) -> PathBuf {
    let stem = path.file_stem().unwrap_or_default().to_string_lossy();
    let name = match path.extension() {
        Some(extension) => format!("{stem}.{n}.{}", extension.to_string_lossy()),
        None => format!("{stem}.{n}"),
    };
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Folder(PathBuf);

    impl Folder {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("rexyra-logfile-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        /// The files in the folder, by name, with what they hold.
        fn contents(&self) -> Vec<(String, String)> {
            let mut files: Vec<_> = fs::read_dir(&self.0)
                .unwrap()
                .map(|entry| {
                    let entry = entry.unwrap();
                    (
                        entry.file_name().to_string_lossy().into_owned(),
                        fs::read_to_string(entry.path()).unwrap(),
                    )
                })
                .collect();
            files.sort();
            files
        }
    }

    impl Drop for Folder {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn named(files: &[(&str, &str)]) -> Vec<(String, String)> {
        files
            .iter()
            .map(|(name, text)| ((*name).to_owned(), (*text).to_owned()))
            .collect()
    }

    #[test]
    fn earlier_files_keep_the_extension() {
        assert_eq!(
            earlier(Path::new(r"C:\logs\rexyra.log"), 2),
            Path::new(r"C:\logs\rexyra.2.log")
        );
        assert_eq!(earlier(Path::new("server"), 1), Path::new("server.1"));
    }

    #[test]
    fn rolls_at_the_size_and_keeps_the_newest_files() {
        let folder = Folder::new("roll");
        let path = folder.0.join("rexyra.log");
        let mut log = RotatingLog::open(&path, 10, 2).unwrap();
        // Lines of four bytes: two fit in ten, a third does not.
        for line in [
            "a01\n", "a02\n", "a03\n", "a04\n", "a05\n", "a06\n", "a07\n",
        ] {
            log.write_all(line.as_bytes()).unwrap();
        }
        assert_eq!(
            folder.contents(),
            named(&[
                ("rexyra.1.log", "a05\na06\n"),
                ("rexyra.2.log", "a03\na04\n"),
                ("rexyra.log", "a07\n"),
            ])
        );
    }

    #[test]
    fn a_line_longer_than_the_limit_gets_a_file_of_its_own() {
        let folder = Folder::new("long");
        let path = folder.0.join("rexyra.log");
        let mut log = RotatingLog::open(&path, 4, 3).unwrap();
        for line in ["ab\n", "a much longer line\n", "cd\n"] {
            log.write_all(line.as_bytes()).unwrap();
        }
        assert_eq!(
            folder.contents(),
            named(&[
                ("rexyra.1.log", "a much longer line\n"),
                ("rexyra.2.log", "ab\n"),
                ("rexyra.log", "cd\n"),
            ])
        );
    }

    #[test]
    fn counts_what_an_earlier_run_wrote() {
        let folder = Folder::new("reopen");
        let path = folder.0.join("rexyra.log");
        fs::write(&path, "earlier run\n").unwrap();
        // A full file is rolled before the run starts, so the new one begins
        // with it, without a blank line.
        let mut log = RotatingLog::open(&path, 12, 1).unwrap();
        log.begin_run().unwrap();
        log.write_all(b"this run\n").unwrap();
        assert_eq!(
            folder.contents(),
            named(&[
                ("rexyra.1.log", "earlier run\n"),
                ("rexyra.log", "this run\n")
            ])
        );
        // With room left, a blank line sets the runs apart.
        let mut log = RotatingLog::open(&path, 100, 1).unwrap();
        log.begin_run().unwrap();
        log.write_all(b"next run\n").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "this run\n\nnext run\n");
    }

    #[test]
    fn keeping_no_earlier_files_starts_the_file_again() {
        let folder = Folder::new("none");
        let path = folder.0.join("rexyra.log");
        let mut log = RotatingLog::open(&path, 8, 0).unwrap();
        for line in ["one\n", "two\n", "three\n"] {
            log.write_all(line.as_bytes()).unwrap();
        }
        assert_eq!(folder.contents(), named(&[("rexyra.log", "three\n")]));
    }

    #[test]
    fn a_size_of_zero_never_rolls() {
        let folder = Folder::new("never");
        let path = folder.0.join("rexyra.log");
        let mut log = RotatingLog::open(&path, 0, 5).unwrap();
        for _ in 0..100 {
            log.write_all(b"line\n").unwrap();
        }
        assert_eq!(folder.contents().len(), 1);
    }

    #[cfg(windows)]
    #[test]
    fn a_file_another_program_holds_is_written_on() {
        use std::os::windows::fs::OpenOptionsExt;
        let folder = Folder::new("held");
        let path = folder.0.join("rexyra.log");
        let mut log = RotatingLog::open(&path, 8, 2).unwrap();
        log.write_all(b"first\n").unwrap();
        // A reader that shares reading and writing but not deleting, which
        // is what stops a rename, as some log viewers do.
        let reader = OpenOptions::new()
            .read(true)
            .share_mode(0x1 | 0x2)
            .open(&path)
            .unwrap();
        log.write_all(b"second\n").unwrap();
        drop(reader);
        let files = folder.contents();
        assert_eq!(files.len(), 1, "{files:?}");
        assert!(files[0].1.starts_with("first\ncould not roll"), "{files:?}");
        assert!(files[0].1.ends_with("second\n"), "{files:?}");
        // Once it is let go, the next roll goes ahead.
        for _ in 0..30 {
            log.write_all(b"more\n").unwrap();
        }
        assert!(folder.0.join("rexyra.1.log").exists());
    }
}
