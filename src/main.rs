use rand::prelude::*;
use redb::{backends::InMemoryBackend, Database, StorageBackend, TableDefinition};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Debug, Clone)]
pub struct TestBackend {
    failure_rate: f64,
    should_fail: Arc<AtomicBool>,
    inner: Arc<InMemoryBackend>,
}

impl StorageBackend for TestBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.maybe_fail()?;
        self.inner.len()
    }

    #[inline]
    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, std::io::Error> {
        self.maybe_fail()?;
        self.inner.read(offset, len)
    }

    #[inline]
    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.maybe_fail()?;
        self.inner.set_len(len)
    }

    #[inline]
    fn sync_data(&self, eventual: bool) -> Result<(), std::io::Error> {
        self.maybe_fail()?;
        self.inner.sync_data(eventual)
    }

    #[inline]
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.maybe_fail()?;
        self.inner.write(offset, data)
    }
}

impl TestBackend {
    pub fn new(failure_rate: f64) -> Self {
        Self {
            failure_rate,
            should_fail: Arc::new(false.into()),
            inner: InMemoryBackend::new().into(),
        }
    }

    #[inline]
    fn maybe_fail(&self) -> Result<(), std::io::Error> {
        let mut rng = thread_rng();

        if !self.should_fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        if rng.gen_range(0f64..self.failure_rate) < self.failure_rate {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated failure",
            ))
        } else {
            Ok(())
        }
    }
}

pub const TABLE: TableDefinition<u128, bool> = TableDefinition::new("notes");

fn run(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = thread_rng();

    loop {
        let text: u128 = rng.gen();
        let w = db.begin_write()?;

        {
            let mut t = w.open_table(TABLE)?;
            t.insert(text, true)?;
        }

        w.commit()?
    }
}

fn main() {
    let backend = TestBackend::new(0.05);
    let should_fail = backend.should_fail.clone();
    let db = redb::Builder::new()
        .create_with_backend(backend.clone())
        .unwrap();

    should_fail.store(true, Ordering::SeqCst);

    loop {
        let _ = run(&db);
    }
}
