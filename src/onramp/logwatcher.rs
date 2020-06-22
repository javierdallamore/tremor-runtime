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

use crate::errors::Result;
use crate::ramp;
use memmap::MmapMut;
use memmap::MmapOptions;
use simd_json::prelude::*;
use std::ops::DerefMut;
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
use std::fs::OpenOptions;
use std::io::Write;
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
    let (mut store, mut obj, restore_state) = if Path::new(&config.restore_file).exists() {
        // is binary
        if config.migrate {
            let (store, obj) = create_mmap(config.restore_file.clone())?;
            let restore_state = RestoreState::from_file(&config.restore_file.clone());
            let restore_state = match restore_state {
                Ok(rs) => rs,
                Err(err) => {
                    error!("restoring state from {}: {:?}", config.restore_file, err);
                    RestoreState::empty()
                }
            };

            (store, obj, restore_state)
        } else {
            let (store, obj) = read_mmap(config.restore_file.clone())?;
            let restore_state = RestoreState::from_json(obj.encode());

            let restore_state = match restore_state {
                Ok(rs) => rs,
                Err(err) => {
                    error!("restoring state from {}: {:?}", config.restore_file, err);
                    RestoreState::empty()
                }
            };

            (store, obj, restore_state)
        }
    } else {
        let (store, obj) = create_mmap(config.restore_file.clone())?;
        (store, obj, RestoreState::empty())
    };

    if let Ok(source_spec) = config.source.to_source_spec() {
        let (walker_sender, source_sender, _join_handles) =
            start_all(content_sender, source_spec, restore_state);

        loop {
            match task::block_on(handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter))? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                PipeHandlerResult::Normal => (),
            }

            match content_receiver.recv_timeout(Duration::from_millis(1000)) {
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

                        if let Err(e) = obj.insert(content.file_path, handler_info.to_line()) {
                            warn!("Could not insert into obj: {}", e);
                        }

                        let string = obj.encode();
                        let bytes = string.as_bytes();
                        let end = bytes.len();

                        let result = [&end.to_be_bytes(), bytes].concat();

                        store.deref_mut().write_all(&result)?;
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

fn read_mmap(restore_file: String) -> Result<(MmapMut, simd_json::value::owned::Value)> {
    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(restore_file)?;

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mut store = mmap.make_mut()?;

    let byteslen = &mut store[0..8];
    let mut array = [0; 8];
    let byteslen = &byteslen[..array.len()];
    array.copy_from_slice(byteslen);

    let len = usize::from_be_bytes(array);
    info!("READ MMAP LEN {}", len);

    let mut bytes = &mut store[8..(len + 8)];
    let obj = simd_json::to_owned_value(&mut bytes)?;

    Ok((store, obj))
}

fn create_mmap(restore_file: String) -> Result<(MmapMut, simd_json::value::owned::Value)> {
    let obj = simd_json::OwnedValue::object();
    let config = ramp::Config {
        path: restore_file,
        size: 4096,
    };

    let p = Path::new(&config.path);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(p)?;
    file.set_len(config.size as u64)?;
    let _len = config.size as usize;
    let string = obj.encode();
    let bytes = string.as_bytes();
    let end = bytes.len();

    let result = [&end.to_be_bytes(), bytes].concat();
    file.write_all(&result)?;

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let store = mmap.make_mut()?;
    Ok((store, obj))
}

#[cfg(test)]
mod tests {
    use crate::errors::Result;
    use std::fs;

    #[test]
    fn check_files() -> Result<()> {
        assert_eq!(fs::metadata("/tmp/noexists").is_ok(), true);
        let logwatcher_restore = fs::metadata("/tmp/logwatcher.json");
        assert_eq!(logwatcher_restore.is_ok(), true);
        Ok({})
    }
}
