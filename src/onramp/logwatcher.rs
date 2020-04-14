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
use chrono::Utc;
use std::thread;
use std::time::Duration;

//import LogWatcherAgent
use regex::{Regex, RegexBuilder, RegexSet, RegexSetBuilder};
use std::fs::File;
//use std::io;
use std::io::prelude::*;
//use std::io::BufReader;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
//use std::thread::sleep;
//use std::time::Duration;
use std::sync::mpsc::sync_channel;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// source logwatcher to read data from, it will be iterated over repeatedly,
    pub source: String,
    // valid lines to consume
    pub regex: Vec<String>,
    // max lines to accumulate to match regex
    pub max_lines: usize,
    // seconds to wait for new lines until send whatever was accumulated
    pub wait_for_new_line: i64,
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
    let hostname = hostname();
    let source = config.source.clone();
    let max_lines = config.max_lines;
    let (loop_tx, loop_rx) = sync_channel::<String>(max_lines);

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-logwatcher".to_string(),
        host: hostname.clone(),
        port: None,
        path: vec![source.clone()],
    };

    info!("Starting LogWatcher");

    let mut log_watcher = LogWatcherAgent::register(
        source.clone(),
        config.regex.clone(),
        config.max_lines,
        config.wait_for_new_line,
    )
    .expect("LogWatcherAgent::register failed");

    std::thread::spawn({
        move || {
            log_watcher.watch(&mut |line: String| {
                loop_tx.send(line).expect("error sending to channel");
            });
        }
    });

    loop {
        match task::block_on(handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter))? {
            PipeHandlerResult::Retry => continue,
            PipeHandlerResult::Terminate => return Ok(()),
            PipeHandlerResult::Normal => (),
        }

        let line = loop_rx.recv().expect("error receiving from channel");
        let mut ingest_ns = nanotime();
        let ts = (ingest_ns / 1_000_000) as u64;

        let data = serde_json::to_vec(&json!({
            "headers": {"hostname": hostname.clone(), "file": source, "ts": ts},
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

#[derive(Debug)]
pub struct LogWatcherAgent {
    filename: String,
    inode: u64,
    pos: u64,
    reader: BufReader<File>,
    finish: bool,
    regex_list: Vec<Regex>,
    regex_set: RegexSet,
    file_lines: Vec<String>,
    max_lines: usize,
    wait_for_new_line: i64,
    last_read: i64,
}

impl LogWatcherAgent {
    pub fn register(
        filename: String,
        regex_str: Vec<String>,
        max_lines: usize,
        wait_for_new_line: i64,
    ) -> Result<LogWatcherAgent> {
        let f = File::open(filename.clone()).expect("error opening file");
        let metadata = f.metadata().expect("error getting file metadata");

        let mut reader = BufReader::new(f);
        let pos = metadata.len();
        reader
            .seek(SeekFrom::Start(pos))
            .expect("seek in file failed");

        let regex_set = RegexSetBuilder::new(&regex_str)
            .dot_matches_new_line(true)
            .multi_line(true)
            .build()
            .expect("wrong regex");

        let regex_list: Vec<Regex> = regex_str
            .into_iter()
            .map(|x| {
                RegexBuilder::new(&x)
                    .dot_matches_new_line(true)
                    .multi_line(true)
                    .build()
                    .expect("wrong regex")
            })
            .collect();

        let logwatcher_agent = LogWatcherAgent {
            filename,
            inode: metadata.ino(),
            pos,
            reader,
            finish: false,
            regex_list,
            regex_set,
            file_lines: Vec::new(),
            max_lines,
            last_read: Utc::now().timestamp(),
            wait_for_new_line,
        };
        Ok(logwatcher_agent)
    }

    fn reopen_if_log_rotated<F: ?Sized>(&mut self, callback: &mut F)
    where
        F: Fn(String),
    {
        loop {
            match File::open(self.filename.clone()) {
                Ok(x) => {
                    let f = x;
                    if let Ok(metadata) = f.metadata() {
                        if metadata.ino() == self.inode {
                            thread::sleep(Duration::new(1, 0));
                        } else {
                            self.finish = true;
                            self.watch(callback);
                            self.finish = false;
                            info!("reloading log file");
                            self.reader = BufReader::new(f);
                            self.pos = 0;
                            self.inode = metadata.ino();
                        }
                        break;
                    } else {
                        thread::sleep(Duration::new(1, 0));
                        continue;
                    }
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
                        self.reader
                            .seek(SeekFrom::Start(self.pos))
                            .expect("seek in file failed");
                        self.last_read = Utc::now().timestamp();

                        self.file_lines.push(line.clone());
                        line.clear();

                        let mut last_match_index = 0;
                        let lines_joined = self.file_lines.join("");
                        let matches: Vec<_> =
                            self.regex_set.matches(&lines_joined).into_iter().collect();

                        if let Some(regex_matched_index) = matches.first() {
                            if let Some(first_regex_matched) =
                                self.regex_list.get(*regex_matched_index)
                            {
                                for mat in first_regex_matched.find_iter(&lines_joined) {
                                    last_match_index = mat.end();
                                    let sub = &lines_joined[mat.start()..last_match_index];
                                    callback(sub.to_string());
                                }
                            }
                        }

                        if last_match_index > 0 {
                            // if something matched, we keep only the lines after the last matched line
                            self.file_lines.clear();
                        }

                        // remove lines while file_lines > max_lines
                        while self.file_lines.len() > self.max_lines {
                            self.file_lines.remove(0);
                        }
                    } else {
                        let secs_since_last_read = Utc::now().timestamp() - self.last_read;
                        if secs_since_last_read > self.wait_for_new_line
                            && !self.file_lines.is_empty()
                        {
                            callback(self.file_lines.join(""));
                            self.file_lines.clear();
                        }

                        if self.finish {
                            break;
                        } else {
                            self.reopen_if_log_rotated(callback);
                            self.reader
                                .seek(SeekFrom::Start(self.pos))
                                .expect("seek in file failed");
                        }
                    }
                }
                Err(err) => {
                    error!("{}", err);
                }
            }
        }
    }
}
