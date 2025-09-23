#![cfg(feature = "alloc-tracing")]

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::{
    fs::{OpenOptions, create_dir_all},
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::interval;
use tracing_core::{
    Event, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id},
};
use tracing_subscriber::{
    layer::{Context, Layer},
    registry::LookupSpan,
};
thread_local! {
    static QCTX: std::cell::RefCell<Option<(String, String)>> =
        const { std::cell::RefCell::new(None) };
}
#[derive(Default)]
struct FieldsGrabber {
    query_id: Option<String>,
    session_id: Option<String>,
    op: Option<String>, // "alloc"/"dealloc"/"realloc"
    addr: Option<u64>,
    size: Option<u64>,
}

impl Visit for FieldsGrabber {
    fn record_i64(&mut self, f: &Field, v: i64) {
        if f.name() == "size" && v >= 0 {
            self.size = Some(v as u64);
        }
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        match f.name() {
            "addr" => self.addr = Some(v),
            "size" => self.size = Some(v),
            _ => {}
        }
    }
    fn record_str(&mut self, f: &Field, v: &str) {
        match f.name() {
            "query_id" => self.query_id = Some(v.to_string()),
            "session_id" => self.session_id = Some(v.to_string()),
            "op" => self.op = Some(v.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, f: &Field, v: &dyn Debug) {
        let s = format!("{v:?}");
        match f.name() {
            "query_id" => self.query_id = Some(s.trim_matches('"').to_string()),
            "session_id" => self.session_id = Some(s.trim_matches('"').to_string()),
            _ => {}
        }
    }
}

#[derive(Default)]
struct AllocSpanInfo {
    query_id: Option<String>,
    session_id: Option<String>,
}

#[derive(Default)]
struct AggEntry {
    allocs: u64,
    deallocs: u64,
    total_bytes: u64,

    current_bytes: u64,
    max_bytes: u64,
    current_allocs: u64,
    max_allocs: u64,

    min_alloc_size: u64,
    max_alloc_size: u64,

    first_ts: Option<u128>,
    last_ts: Option<u128>,
}

#[derive(Clone)]
pub struct AllocLogLayer {
    agg: Arc<Mutex<HashMap<(String, String), AggEntry>>>,
    file: Arc<Mutex<std::fs::File>>,
}

impl AllocLogLayer {
    pub fn write_to_file(path: &str) -> std::io::Result<Self> {
        if let Some(dir) = Path::new(path).parent() {
            let _ = create_dir_all(dir);
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            agg: Arc::new(Mutex::new(HashMap::new())),
            file: Arc::new(Mutex::new(file)),
        })
    }

    #[inline]
    fn ts() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0)
    }

    pub fn spawn_flusher(self: Arc<Self>, interval_secs: Duration) {
        let agg = self.agg.clone();
        let file = self.file.clone();

        tokio::spawn(async move {
            let mut ticker = interval(interval_secs);
            loop {
                ticker.tick().await;

                if let Ok(mut map) = agg.lock() {
                    if map.is_empty() {
                        continue;
                    }
                    if let Ok(mut file) = file.lock() {
                        for ((qid, sid), entry) in map.drain() {
                            let _ = writeln!(
                                &mut *file,
                                "ts={} qid={} sid={} allocs={} deallocs={} total_bytes={} current_bytes={} max_bytes={} current_allocs={} max_allocs={} min_alloc_size={} max_alloc_size={} first_ts={:?} last_ts={:?}",
                                Self::ts(),
                                qid,
                                sid,
                                entry.allocs,
                                entry.deallocs,
                                entry.total_bytes,
                                entry.current_bytes,
                                entry.max_bytes,
                                entry.current_allocs,
                                entry.max_allocs,
                                entry.min_alloc_size,
                                entry.max_alloc_size,
                                entry.first_ts,
                                entry.last_ts,
                            );
                        }
                        let _ = file.flush();
                    }
                }
            }
        });
    }
}

impl<S> Layer<S> for AllocLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let meta = attrs.metadata();
        if meta.target() == "alloc"
            && meta.name() == "query_alloc"
            && let Some(span) = ctx.span(id)
        {
            let mut g = FieldsGrabber::default();
            attrs.record(&mut g);
            let mut ext = span.extensions_mut();
            ext.insert(AllocSpanInfo {
                query_id: g.query_id.take(),
                session_id: g.session_id.take(),
            });
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        if meta.target() != "tracing_allocations" {
            return;
        }

        let mut g = FieldsGrabber::default();
        event.record(&mut g);

        if let Some((qid, sid)) = QCTX
            .try_with(|cell| cell.try_borrow().ok().and_then(|c| c.clone()))
            .ok()
            .flatten()
        {
            let op = g.op.as_deref().unwrap_or("alloc");
            let size = g.size.unwrap_or(0);

            if let Ok(mut map) = self.agg.lock() {
                let entry = map.entry((qid, sid)).or_default();
                match op {
                    "alloc" => {
                        entry.allocs += 1;
                        entry.total_bytes += size;
                        entry.current_allocs += 1;
                        entry.current_bytes += size;

                        if entry.current_bytes > entry.max_bytes {
                            entry.max_bytes = entry.current_bytes;
                        }
                        if entry.current_allocs > entry.max_allocs {
                            entry.max_allocs = entry.current_allocs;
                        }
                        if entry.min_alloc_size == 0 || size < entry.min_alloc_size {
                            entry.min_alloc_size = size;
                        }
                        if size > entry.max_alloc_size {
                            entry.max_alloc_size = size;
                        }
                    }
                    "dealloc" => {
                        entry.deallocs += 1;
                        if entry.current_allocs > 0 {
                            entry.current_allocs -= 1;
                        }
                        if entry.current_bytes >= size {
                            entry.current_bytes -= size;
                        }
                    }
                    _ => {}
                }

                let ts = Self::ts();
                if entry.first_ts.is_none() {
                    entry.first_ts = Some(ts);
                }
                entry.last_ts = Some(ts);
            }
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(info) = span.extensions().get::<AllocSpanInfo>()
        {
            let q = info.query_id.clone().unwrap_or_else(|| "-".to_string());
            let s = info.session_id.clone().unwrap_or_else(|| "-".to_string());

            let _ = QCTX.try_with(|cell| {
                if let Ok(mut c) = cell.try_borrow_mut() {
                    *c = Some((q, s));
                }
            });
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && span.extensions().get::<AllocSpanInfo>().is_some()
        {
            let _ = QCTX.try_with(|cell| {
                if let Ok(mut c) = cell.try_borrow_mut() {
                    *c = None;
                }
            });
        }
    }
}
