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
use serde_yaml::Value;
//use std::process;
use std::{thread, time};

use crate::errors::Result;
use crate::ramp;
use simd_json::prelude::*;
use std::collections::HashMap;
use std::path::Path;

mod config;
mod handlers;
mod input;
mod path;
mod process;
mod restore;
extern crate serde_humanize_rs;

use config::{Config, SourceSpec};
use crossbeam_channel::{bounded, RecvTimeoutError, Sender};
use input::Content;
use process::HandlerInfo;
use process::ProcessInfo;
use restore::RestoreState;
use simd_json::json;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct LogWatcher {
    pub config: Config,
}

impl onramp::Impl for LogWatcher {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();
    let mut id = 0;
    let (content_sender, content_receiver) = bounded(100);

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-logwatcher".to_string(),
        host: hostname(),
        port: None,
        path: vec![config.source.path.clone()],
    };

    info!("starting logwatcher");
    let (mut cache, restore_state) = load_restore_file(config.restore_file.clone())?;

    if let Ok(source_spec) = config.source.to_source_spec() {
        let eviction_interval = source_spec.eviction_interval;
        let (_walker_sender, _source_sender, _join_handles) =
            start_all(content_sender, source_spec, restore_state);

        let mut last_eviction: Option<Instant> = None;
        loop {
            match task::block_on(handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter))? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                PipeHandlerResult::Normal => (),
            }

            if let Err(e) =
                evict_removed_files_from_restore(&mut cache, eviction_interval, &mut last_eviction)
            {
                metrics_reporter.increment_error();
                warn!("evict removed file from restore error: {}", e);
            };

            match content_receiver.recv_timeout(Duration::from_millis(100)) {
                Ok((content, handler_info)) => {
                    let data = simd_json::to_vec(&json!(content));

                    let mut ingest_ns = nanotime();

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
                        id += 1;

                        if let Err(e) = update_restore_file(&mut cache, content, handler_info) {
                            error!("failed to update restore file: {}", e);
                        }

                        if config.throttle > 0 {
                            thread::sleep(time::Duration::from_millis(config.throttle));
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => trace!("recv timeout"),
                Err(error) => {
                    error!("Request error {}", error);
                }
            }
        }
    }
    Ok(())
}

impl Onramp for LogWatcher {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;
        thread::Builder::new()
            .name(format!("onramp-logwatcher-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(&rx, &config, preprocessors, codec, metrics_reporter) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}

fn start_all(
    content_sender: Sender<(Content, HandlerInfo)>,
    source: SourceSpec,
    restore_state: RestoreState,
) -> (
    Sender<path::WalkerMsg>,
    Sender<handlers::Msg>,
    Vec<JoinHandle<ProcessInfo>>,
) {
    let mut join_handles = vec![];
    let handler_receive_timeout = Duration::from_millis(5);

    let matcher = path::MatchRules::new(source.rules.clone());
    let (walker_sender, walker_receiver) = bounded(0);
    let (walker_msg_sender, walker_msg_receiver) = bounded(0);

    let walker_handle = path::walkdir_filter_send_thread(
        &source.path,
        matcher,
        walker_sender,
        walker_msg_receiver,
        source.walk_interval,
    );

    let (source_sender, source_receiver) = bounded(0);
    let source_handle = path::ChangeWatcher::from_restore_state(&restore_state).start(
        walker_receiver,
        source_sender.clone(),
        source.receive_timeout,
        source.check_interval,
        source.eviction_interval,
        source.evict_older_than,
    );

    let handlers = handlers::Handlers::start(
        content_sender.clone(),
        source_receiver,
        handler_receive_timeout,
        restore_state.clone(),
        source.max_lines,
        source.line_regex.clone(),
    );
    join_handles.push(handlers);
    join_handles.push(walker_handle);
    join_handles.push(source_handle);

    (walker_msg_sender, source_sender.clone(), join_handles)
}

fn update_restore_file(
    cache: &mut Box<dyn ramp::KV>,
    content: Content,
    handler_info: HandlerInfo,
) -> Result<()> {
    let mut obj = cache.get()?;
    let handler_info_line = handler_info.to_line();
    if let Err(e) = obj.insert(content.file_path, handler_info_line) {
        return Err(format!("Could not insert into obj: {}", e).into());
    }

    cache.set(obj)?;
    Ok(())
}

fn load_restore_file(config: ramp::Config) -> Result<(Box<dyn ramp::KV>, RestoreState)> {
    info!("loading restore file");
    let obj = simd_json::OwnedValue::object();
    let mut cache = match ramp::lookup("mmap_restore_file", Some(config.clone()), &obj) {
        Ok(v) => v,
        Err(e) => return Err(e),
    };

    let obj = cache.get()?;
    let restore_state = match RestoreState::from_json(obj.encode()) {
        Ok(rs) => rs,
        Err(err) => {
            error!("restoring state from {}: {:?}", config.path, err);
            RestoreState::empty()
        }
    };

    Ok((cache, restore_state))
}

fn evict_removed_files_from_restore(
    cache: &mut Box<dyn ramp::KV>,
    eviction_interval: Duration,
    last_eviction: &mut Option<Instant>,
) -> Result<()> {
    if last_eviction.is_none() || last_eviction.unwrap().elapsed() > eviction_interval {
        info!("checking files to remove from restore");
        *last_eviction = Some(Instant::now());

        let mut obj = cache.get()?;

        let mut restore_data = obj.encode();
        let restore_data_kv: HashMap<&str, &str> = simd_json::from_str(&mut restore_data).unwrap();
        for (file_path, _) in restore_data_kv.iter() {
            if !Path::new(&file_path).exists() {
                match obj.remove(*file_path) {
                    Ok(v) => info!(
                        "removed file {} from restore because was deleted {:?}",
                        file_path, v
                    ),
                    Err(_) => error!("couldn't removed file {} from restore", file_path),
                };
            }
        }

        cache.set(obj)?;
    }
    Ok(())
}
