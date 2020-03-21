// Copyright 2018-2020, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::onramp::prelude::*;
//NOTE: This is required for StreamHander's stream
use crate::utils::nanotime;
use serde_yaml::Value;
use simd_json::json;
use std::time::Duration;

// Metrics imports
use serde::{Deserialize, Serialize};

// MountInfo imports
// SysInfo Imports
// ProcInfo imports
use std::ffi::CString;
use std::path::PathBuf;
use libc;
use proc_mounts::{self, MountIter};


#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// Interval in milliseconds
    pub interval: u64,
    pub metric: MetricType,
}

impl ConfigImpl for Config {}

pub struct OSMetric {
    pub config: Config,
}

impl onramp::Impl for OSMetric {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for osmetric onramp".into())
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MetricType {
    Sys,
    Proc,
    Mount,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Metric {
    Sys(SysInfo),
    Proc(ProcInfo),
    Mount(MountInfo),
}

fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();
    let mut id: u64 = 0;
    let host_name = hostname();

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-osmetric".to_string(),
        host: hostname(),
        port: None,
        path: vec![config.interval.to_string()],
    };

    loop {
        match task::block_on(handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter))? {
            PipeHandlerResult::Retry => continue,
            PipeHandlerResult::Terminate => return Ok(()),
            PipeHandlerResult::Normal => (),
        }

        thread::sleep(Duration::from_millis(config.interval));
        let mut ingest_ns = nanotime();
        let ts = (ingest_ns / 1000000) as i64;

        let metrics: Vec<Metric> = match config.metric {
            MetricType::Sys => SysInfo::get_metrics(),
            MetricType::Proc => ProcInfo::get_metrics(),
            MetricType::Mount => MountInfo::get_metrics(),
        };

        for metric in &metrics {
            let data = serde_json::to_vec(&json!({
                "headers": {"hostname": host_name, "metric": config.metric, "ts": ts},
                "body": metric,
            }));

            if let Ok(data) = data {
                send_event(
                    &pipelines,
                    &mut preprocessors,
                    &mut codec,
                    &mut metrics_reporter,
                    &mut ingest_ns,
                    &origin_uri,
                    id,
                    data,
                );
            }

            id += 1;
        }
    }
}

impl Onramp for OSMetric {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let config = self.config.clone();
        let (tx, rx) = channel(1);
        let codec = codec::lookup(codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;
        thread::Builder::new()
            .name(format!("onramp-osmetric-{}", "???"))
            .spawn(move || onramp_loop(&rx, &config, preprocessors, codec, metrics_reporter))?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

// MountInfo
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MountInfo {
    source: String,
    dest: String,
    fstype: String,
    options: String,
    dump: i32,
    pass: i32,
    used: u64,
    available: u64,
    total: u64,
    use_pc: u32,
}

impl MountInfo {
    pub fn new(
        source: PathBuf,
        dest: PathBuf,
        fstype: &str,
        options: &Vec<String>,
        dump: i32,
        pass: i32,
    ) -> MountInfo {
        let dest_str = dest.to_string_lossy();
        let (used, available, total, use_pc) = fs_usage(&dest_str);
        MountInfo {
            source: String::from(source.to_string_lossy()),
            dest: String::from(dest_str),
            fstype: String::from(fstype),
            options: options.join(";"),
            dump,
            pass,
            used,
            available,
            total,
            use_pc,
        }
    }

    pub fn get_metrics() -> Vec<Metric> {
        let mut metrics = Vec::new();
        match MountIter::new() {
            Ok(mount_iter) => {
                for mount in mount_iter {
                    match mount {
                        Ok(proc_mounts::MountInfo {
                            source,
                            dest,
                            fstype,
                            options,
                            dump,
                            pass,
                        }) => {
                            let mount_info = MountInfo::new(
                                source, dest, &fstype, &options, dump, pass,
                            );
                            metrics.push(Metric::Mount(mount_info));
                        }
                        Err(err) => {
                            error!("Error reading mount info: {}", err);
                        }
                    }
                }
            }
            Err(err) => {
                error!("Error reading mount info: {}", err);
            }
        }
        metrics
    }
}

pub fn statvfs(mount_point: &str) -> Option<libc::statvfs> {
    unsafe {
        let mountp = CString::new(mount_point).unwrap();
        let mut stats: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(mountp.as_ptr(), &mut stats) != 0 {
            None
        } else {
            Some(stats)
        }
    }
}

pub fn fs_usage(mount_point: &str) -> (u64, u64, u64, u32) {
    match statvfs(mount_point) {
        Some(stats) => {
            let total = stats.f_blocks * stats.f_frsize / 1024;
            let available = stats.f_bavail * stats.f_frsize / 1024;
            let free = stats.f_bfree * stats.f_frsize / 1024;
            let used = total - free;
            let u100 = used * 100;
            let nonroot_total = used + available;
            let pct = if nonroot_total == 0 {
                0
            } else {
                u100 / nonroot_total
            };

            (used, available, total, pct as u32)
        }
        None => (0, 0, 0, 100),
    }


}

// ProcInfo
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProcInfo {
    pid: i32,
    owner: u32,
    open_fd_count: i64,
    num_threads: i64,
    starttime: i64,
    utime: u64,
    stime: u64,
    cmdline: String,
}

impl ProcInfo {
    pub fn new(proc: &procfs::Process) -> ProcInfo {
        let open_fd_count = match proc.fd() {
            Ok(fds) => fds.len() as i64,
            Err(_) => -1,
        };

        let cmdline = match proc.cmdline() {
            Ok(items) => {
                if items.len() == 0 {
                    String::from("?")
                } else {
                    items.join(" ")
                }
            }
            Err(_) => String::from("?"),
        };

        ProcInfo {
            pid: proc.stat.pid,
            owner: proc.owner,
            open_fd_count,
            num_threads: proc.stat.num_threads,
            starttime: proc.stat.starttime,
            utime: proc.stat.utime,
            stime: proc.stat.stime,
            cmdline: cmdline,
        }
    }

    pub fn get_metrics() ->  Vec<Metric> {
        let mut metrics = Vec::new();
        for process in procfs::all_processes() {
            let proc_info = ProcInfo::new(&process);
            metrics.push(Metric::Proc(proc_info))
        }

        metrics
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SysInfo {
    mem_total: Option<u64>,
    mem_free: Option<u64>,
    mem_buffers: Option<u64>,
    mem_cached: Option<u64>,
    load_avg_1: Option<f32>,
    load_avg_5: Option<f32>,
    load_avg_15: Option<f32>,
}

impl SysInfo {
    pub fn new() -> SysInfo {
        let (mem_total, mem_free, mem_buffers, mem_cached) = match procfs::meminfo() {
            Ok(mi) => (
                Some(mi.mem_total),
                Some(mi.mem_free),
                Some(mi.buffers),
                Some(mi.cached),
            ),
            Err(err) => {
                eprintln!("Error loading meminfo: {}", err);
                (None, None, None, None)
            }
        };

        let (load_avg_1, load_avg_5, load_avg_15) = match procfs::LoadAverage::new() {
            Ok(la) => (Some(la.one), Some(la.five), Some(la.fifteen)),
            Err(err) => {
                eprintln!("Error loading load avg: {}", err);
                (None, None, None)
            }
        };

        SysInfo {
            mem_total,
            mem_free,
            mem_buffers,
            mem_cached,
            load_avg_1,
            load_avg_5,
            load_avg_15,
        }
    }

    pub fn get_metrics() -> Vec<Metric> {
        let sys_info = SysInfo::new();
        vec![Metric::Sys(sys_info)]
    }
}