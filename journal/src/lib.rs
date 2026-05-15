use std::io;
use std::path::{Path, PathBuf};

use maple_types::Order;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub seq: u64,
    pub order: Order,
}

pub struct Journal {
    _path: PathBuf,
}

impl Journal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            _path: path.as_ref().to_path_buf(),
        })
    }

    pub fn append(&mut self, seq: u64, order: &Order) -> io::Result<()> {
        let _entry = JournalEntry {
            seq,
            order: order.clone(),
        };

        // TODO: write as JSON line to disk and flush.
        Ok(())
    }

    // TODO: implement read and replay functionality for recovery.
}