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

use crate::dflt;
use crate::onramp::prelude::*;
use serde_yaml::Value;
use std::io::{BufRead, BufReader};
//use std::process;
use std::thread;
use std::time::Duration;

//import LogWatcherAgent
use std::fs::File;
//use std::io;
use std::io::prelude::*;
//use std::io::BufReader;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
//use std::thread::sleep;
//use std::time::Duration;

use std::sync::{Arc, Mutex};

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// source logwatcher to read data from, it will be iterated over repeatedly,
    /// can be xz compressed
    pub source: String,
    #[serde(default = "dflt::d_false")]
    pub close_on_done: bool,
}

impl ConfigImpl for Config {}

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
    let source = config.source.clone();
    let mut log_watcher = LogWatcherAgent::register(source).unwrap();

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-logwatcher".to_string(),
        host: hostname(),
        port: None,
        path: vec![config.source.clone()],
    };

    info!("Starting LogWatcher");
    let lines = Arc::new(Mutex::new(vec![]));
    std::thread::spawn({
        let clone = Arc::clone(&lines);
        move || {
            log_watcher.watch(&mut |line: String| {
                info!("Line read {}", &line);
                let mut l = clone.lock().unwrap();
                l.push(line);
            });
        }
    });

    loop {
        match task::block_on(handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter))? {
            PipeHandlerResult::Retry => continue,
            PipeHandlerResult::Terminate => return Ok(()),
            PipeHandlerResult::Normal => (),
        }

        let clone = Arc::clone(&lines);
        let mut linesb = clone.lock().unwrap();
        while let Some(line) = linesb.pop() {
            let mut ingest_ns = nanotime();
            let ts = (ingest_ns / 1000000) as i64;

            let data = serde_json::to_vec(&json!({
                "headers": {"hostname": hostname(), "file": config.source, "ts": ts},
                "body": line,
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
                id += 1;
            }
        }
    }
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

pub struct LogWatcherAgent {
    filename: String,
    inode: u64,
    pos: u64,
    reader: BufReader<File>,
    finish: bool,
}

impl LogWatcherAgent {
    pub fn register(filename: String) -> Result<LogWatcherAgent> {
        let f = File::open(filename.clone()).unwrap();
        let metadata = f.metadata().unwrap();

        let mut reader = BufReader::new(f);
        let pos = metadata.len();
        reader.seek(SeekFrom::Start(pos)).unwrap();
        Ok(LogWatcherAgent {
            filename: filename,
            inode: metadata.ino(),
            pos: pos,
            reader: reader,
            finish: false,
        })
    }

    fn reopen_if_log_rotated<F: ?Sized>(&mut self, callback: &mut F)
    where
        F: Fn(String),
    {
        loop {
            match File::open(self.filename.clone()) {
                Ok(x) => {
                    let f = x;
                    let metadata = match f.metadata() {
                        Ok(m) => m,
                        Err(_) => {
                            thread::sleep(Duration::new(1, 0));
                            continue;
                        }
                    };
                    if metadata.ino() != self.inode {
                        self.finish = true;
                        self.watch(callback);
                        self.finish = false;
                        println!("reloading log file");
                        self.reader = BufReader::new(f);
                        self.pos = 0;
                        self.inode = metadata.ino();
                    } else {
                        thread::sleep(Duration::new(1, 0));
                    }
                    break;
                }
                Err(err) => {
                    if err.kind() == ErrorKind::NotFound {
                        thread::sleep(Duration::new(1, 0));
                        continue;
                    }
                }
            };
        }
    }

    pub fn watch<F: ?Sized>(&mut self, callback: &mut F)
    where
        F: Fn(String),
    {
        loop {
            let mut line = String::new();
            let resp = self.reader.read_line(&mut line);
            match resp {
                Ok(len) => {
                    if len > 0 {
                        self.pos += len as u64;
                        self.reader.seek(SeekFrom::Start(self.pos)).unwrap();
                        callback(line.replace("\n", ""));
                        line.clear();
                    } else {
                        if self.finish {
                            break;
                        } else {
                            self.reopen_if_log_rotated(callback);
                            self.reader.seek(SeekFrom::Start(self.pos)).unwrap();
                        }
                    }
                }
                Err(err) => {
                    println!("{}", err);
                }
            }
        }
    }
}
