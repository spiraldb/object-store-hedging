//! Hedged request support for [`object_store`].
//!
//! This crate provides request hedging for read requests, a technique for
//! reducing tail latencies in distributed systems as explored in
//! [The Tail at Scale] and [AnyBlob].
//!
//! # Example
//!
//! ```ignore
//! use object_store::{ObjectStore, ObjectStoreExt, path::Path};
//! use object_store_hedging::{HedgedStore, HedgingConfig};
//!
//! // Wrap any ObjectStore with hedging
//! let store = object_store::aws::AmazonS3Builder::from_env()
//!     .with_bucket_name("my-bucket")
//!     .build()?;
//! let store = HedgedStore::new(store, HedgingConfig::default());
//!
//! // Use as normal - hedging happens automatically on reads
//! let data = store.get(&Path::from("my-key")).await?;
//! ```
//!
//! [`object_store`]: https://github.com/apache/arrow-rs-object-store
//! [The Tail at Scale]: https://research.google/pubs/the-tail-at-scale
//! [AnyBlob]: https://www.vldb.org/pvldb/vol16/p2769-durner.pdf

#![deny(missing_docs)]

use parking_lot::RwLock;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::Result;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, path::Path,
};
use sketches_ddsketch::DDSketch;

/// Configuration for [`HedgedStore`].
pub struct HedgingConfig {
    /// Latency quantile for hedge timing (e.g., 0.95 = p95), must be between [0.0, 1.0],
    /// otherwise will fallback to `warmup_delay`
    pub quantile: f64,
    /// Samples needed before adaptive hedging kicks in
    pub min_samples: usize,
    /// Hedge delay during warmup period
    pub warmup_delay: Duration,
}

// Values here are vaguely taken from AnyBlob
impl Default for HedgingConfig {
    fn default() -> Self {
        Self {
            quantile: 0.95,
            min_samples: 10,
            warmup_delay: Duration::from_millis(200),
        }
    }
}

/// An [`ObjectStore`] wrapper that hedges [`ObjectStore::get_opts`] requests,
/// issuing additional requests if time-to-first-byte it determined to be too long.
/// All other requests are just delegated to the underlying store.
pub struct HedgedStore<T> {
    inner: T,
    sketch: Arc<RwLock<DDSketch>>,
    config: HedgingConfig,
    counter: Arc<AtomicUsize>,
}

impl<T> HedgedStore<T> {
    /// Creates a new instance.
    pub fn new(inner: T, config: HedgingConfig) -> Self {
        Self {
            inner,
            sketch: Arc::new(RwLock::new(DDSketch::default())),
            config,
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<T: ObjectStore> std::fmt::Debug for HedgedStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HedgedObjectStorage")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<T: ObjectStore> std::fmt::Display for HedgedStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HedgedStore({})", self.inner)
    }
}

/// Sleep only if non-zero duration
async fn sleep(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl<T: ObjectStore> ObjectStore for HedgedStore<T> {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let delay = if self.counter.fetch_add(1, Ordering::Relaxed) < self.config.min_samples {
            self.config.warmup_delay
        } else {
            // If the sketch is empty or quantile is out-of-bounds, we fall back to the warmup_delay
            self.sketch
                .read()
                .quantile(self.config.quantile)
                .ok()
                .flatten()
                .map(Duration::from_secs_f64)
                .unwrap_or(self.config.warmup_delay)
        };

        let f1 = async {
            let t = Instant::now();
            let r = self.inner.get_opts(location, options.clone()).await?;
            Result::<_, object_store::Error>::Ok((r, t.elapsed()))
        };
        let f2 = async {
            sleep(delay).await;
            let t = Instant::now();
            let r = self.inner.get_opts(location, options.clone()).await?;
            Result::<_, object_store::Error>::Ok((r, t.elapsed()))
        };

        let (result, time) = tokio::select! {
            r = f1 => {
                r?
            }
            r = f2 => {
                r?
            }
        };

        self.sketch.write().add(time.as_secs_f64());

        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |f| futures::future::ready(f.location > offset))
            .boxed()
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    /// Mock store with controllable delay
    #[derive(Debug)]
    struct MockStore {
        delay: Duration,
        call_count: AtomicUsize,
    }

    impl MockStore {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                call_count: AtomicUsize::new(0),
            }
        }
    }

    impl std::fmt::Display for MockStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockStore")
        }
    }

    #[async_trait]
    impl ObjectStore for MockStore {
        async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Err(object_store::Error::NotFound {
                path: "mock".to_string(),
                source: "mock response".into(),
            })
        }

        async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _: &Path,
            _: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        fn delete_stream(
            &self,
            _: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            unimplemented!()
        }

        fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
            unimplemented!()
        }

        async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
            unimplemented!()
        }
    }

    #[fixture]
    fn default_config() -> HedgingConfig {
        HedgingConfig::default()
    }

    #[rstest]
    #[tokio::test]
    async fn test_uses_default_delay_during_warmup(default_config: HedgingConfig) {
        let mock = MockStore::new(Duration::from_millis(10)); // Fast response
        let store = HedgedStore::new(mock, default_config);

        // First request should use default 200ms delay for hedge
        let _ = store
            .get_opts(&Path::from("test"), GetOptions::default())
            .await;

        // Counter should have incremented
        assert_eq!(store.counter.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_hedged_request_fires_after_delay() {
        let config = HedgingConfig {
            min_samples: 0, // Use sketch immediately
            quantile: 0.5,
            warmup_delay: Duration::from_millis(200),
        };
        let mock = MockStore::new(Duration::from_millis(500)); // Slow - 500ms
        let store = HedgedStore::new(mock, config);

        store.sketch.write().add(0.01);

        let start = Instant::now();
        let _ = store
            .get_opts(&Path::from("test"), GetOptions::default())
            .await;
        let elapsed = start.elapsed();

        // Both requests take 500ms, but they race.
        // The hedged request starts after ~10ms, so total should be ~510ms
        // But since first also takes 500ms, whichever finishes first wins
        // Both should finish around same time (~500-510ms)
        assert!(elapsed < Duration::from_millis(600));
        assert_eq!(store.inner.call_count.load(Ordering::Relaxed), 2);
    }
}
