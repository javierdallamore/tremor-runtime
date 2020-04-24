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
use std::thread;

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
use process::ProcessInfo;
use restore::RestoreState;
use std::fs::OpenOptions;
use std::io::{LineWriter, Write};
use std::thread::JoinHandle;
use std::time::Duration;

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
    let (content_sender, content_receiver) = bounded(0);

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-logwatcher".to_string(),
        host: hostname(),
        port: None,
        path: vec![config.source.path.clone()],
    };

    info!("starting logwatcher");

    info!("get file consumed state from file");
    let restore_state = match &config.restore_file {
        Some(path) => match RestoreState::from_file(&path) {
            Ok(rs) => rs,
            Err(err) => {
                error!("restoring state from {}: {:?}", path, err);
                RestoreState::empty()
            }
        },
        None => RestoreState::empty(),
    };

    if let Ok(source_spec) = config.source.to_source_spec() {
        let (walker_sender, source_sender, join_handles) =
            start_all(content_sender, source_spec, restore_state);

        loop {
            match task::block_on(handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter))? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                PipeHandlerResult::Normal => (),
            }

            match content_receiver.recv_timeout(Duration::from_millis(1000)) {
                Ok(content) => {
                    let data = serde_json::to_vec(&json!(content));

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
                    }
                }
                Err(RecvTimeoutError::Timeout) => trace!("recv timeout"),
                Err(error) => {
                    error!("{}", error);
                    // TODO until I discover how to subscribe to process kill and call the methods bellow
                    break;
                }
            }
        }

        // TODO I do not know how where stop all and store file state before closing
        info!("stopping everything and saving state");

        match walker_sender.send(path::WalkerMsg::Stop) {
            Ok(_) => info!("ok sending stop signal to walker"),
            Err(err) => error!("Error sending stop signal to walker: {}", err),
        }

        match source_sender.send(handlers::Msg::Stop) {
            Ok(_) => info!("ok sending stop signal to source"),
            Err(err) => error!("error sending stop signal to source: {}", err),
        }

        wait_all(join_handles, config.restore_file.clone());
        info!("all threads finished, exiting");
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
    content_sender: Sender<Content>,
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

struct LogWrite;

impl LogWrite {
    fn new() -> LogWrite {
        LogWrite {}
    }
}

impl std::io::Write for LogWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match std::str::from_utf8(buf) {
            Ok(s) => {
                info!("{}", s);
                Ok(buf.len())
            }
            Err(_) => Ok(0),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn wait_all(join_handles: Vec<JoinHandle<ProcessInfo>>, restore_path: Option<String>) {
    let mut writer = match restore_path {
        Some(file_path) => match OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)
        {
            Ok(write_file) => LineWriter::new(Box::new(write_file) as Box<dyn Write>),
            Err(err) => {
                error!("can't open restore file {}: {}", file_path, err);
                LineWriter::new(Box::new(LogWrite::new()) as Box<dyn Write>)
            }
        },
        None => LineWriter::new(Box::new(LogWrite::new()) as Box<dyn Write>),
    };
    for handle in join_handles {
        match handle.join() {
            Ok(ProcessInfo::Nothing) => {
                info!("no process info");
            }
            Ok(ProcessInfo::HandlerInfo(infos)) => {
                for info in infos {
                    if let Some(line) = info.to_line() {
                        match writer.write_all(line.as_bytes()) {
                            Ok(_) => {
                                info!("restore written for file {}", info.path);
                            }
                            Err(err) => {
                                error!("{}", err);
                            }
                        }
                    }
                }
            }
            Err(err) => error!("stop error: {:?}", err),
        };
    }
}
