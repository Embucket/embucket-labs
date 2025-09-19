use core_history::QueryRecordId;
use dashmap::DashMap;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::io::Write;
use std::{fs, sync::Arc, time::Duration};
use tokio::task;

#[derive(Default, Clone, Debug)]
pub struct QueryMemStat {
    pub max_rss: u64,
    pub max_pool_reserved: usize,
}

pub static QUERY_HWM: std::sync::LazyLock<DashMap<QueryRecordId, QueryMemStat>> =
    std::sync::LazyLock::new(DashMap::new);

fn read_proc_rss_bytes() -> Option<u64> {
    let s = fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

fn read_mem_available_bytes() -> Option<u64> {
    let s = fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

pub struct QueryMemGuard {
    handle: task::JoinHandle<()>,
    stop_tx: tokio::sync::oneshot::Sender<()>,
}

#[allow(clippy::cast_precision_loss, clippy::as_conversions)]
impl QueryMemGuard {
    /// `suspect_mb_per_s` — RSS memory usage speed
    #[must_use]
    pub fn start(
        query_id: QueryRecordId,
        runtime_env: Arc<RuntimeEnv>,
        sql_preview: String,
        suspect_mb_per_s: u64,
        tick_ms: u64,
        log_path: Option<String>,
    ) -> Self {
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

        let handle = task::spawn(async move {
            let mut prev_rss = read_proc_rss_bytes().unwrap_or(0);
            let mut last_ts = std::time::Instant::now();
            let mem_available_bytes = read_mem_available_bytes().unwrap_or(0);

            // file log
            let mut file = log_path.and_then(|p| {
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)
                    .ok()
            });

            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    () = tokio::time::sleep(Duration::from_millis(tick_ms)) => {
                        let now = std::time::Instant::now();
                        let dt = now.duration_since(last_ts).as_secs_f64().max(1e-6);
                        last_ts = now;

                        let rss = read_proc_rss_bytes().unwrap_or(0);
                        let pool = runtime_env.memory_pool.reserved();

                        // Renew HWM
                        let mut entry = QUERY_HWM.entry(query_id).or_default();
                        {
                            let s = entry.value_mut();
                            if rss > s.max_rss { s.max_rss = rss; }
                            if pool > s.max_pool_reserved { s.max_pool_reserved = pool; }
                        }

                        // Speed of increasing RSS (MB/s)
                        let drss = rss.saturating_sub(prev_rss);
                        let mb_per_s = (drss as f64 / (1024.0*1024.0)) / dt;
                        prev_rss = rss;

                        // tracing
                        tracing::info!(target="mem_watch.query",
                            %query_id, rss_bytes=rss, pool_reserved_bytes=pool,
                            mb_per_s = format_args!("{:.1}", mb_per_s),
                            "query tick");

                        // file
                        if let Some(f) = file.as_mut() {
                            let _ = writeln!(
                                f,
                                "qid={query_id} rss={rss} mem_avail={mem_available_bytes} pool={pool} mb_per_s={mb_per_s:.1} sql={sql_preview}",
                            );
                            let _ = f.flush();
                        }

                        // SUSPECT for aggressive grow
                        if mb_per_s >= suspect_mb_per_s as f64 {
                            tracing::warn!(target="mem_watch.query",
                                %query_id,
                                mb_per_s = format_args!("{:.1}", mb_per_s),
                                sql = %sql_preview,
                                "SUSPECT: fast RSS growth");
                        }
                    }
                }
            }

            if let Some(stat) = QUERY_HWM.get(&query_id) {
                tracing::info!(target="mem_watch.query",
                    %query_id, max_rss=stat.max_rss, max_pool_reserved=stat.max_pool_reserved,
                    "query finished");
            }
        });

        Self { handle, stop_tx }
    }

    pub async fn finish(self) {
        let _ = self.stop_tx.send(());
        let _ = self.handle.await;
    }
}

#[must_use]
pub fn truncate_sql(sql: &str, max: usize) -> String {
    if sql.len() <= max {
        sql.to_string()
    } else {
        format!("{}…", &sql[..max])
    }
}
