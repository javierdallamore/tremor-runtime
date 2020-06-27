use super::config::LineRegex;
use super::input::Content;
use super::input::{ContentChecksum, Error, FileState, MsgInput};
use super::process::HandlerInfo;
use super::process::ProcessInfo;
use super::restore::RestoreState;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use regex::{Regex, RegexBuilder};
use std::collections::HashMap;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub enum Msg {
    ModifiedChange(String, u64, SystemTime, SystemTime),
    SizeSmaller(String, u64, SystemTime, SystemTime),
    CreatedChange(String, u64, SystemTime, SystemTime),
    #[allow(dead_code)]
    Stop,
}

impl Msg {
    pub fn info(&self) -> Option<(&str, u64, SystemTime, SystemTime)> {
        match self {
            Msg::ModifiedChange(path, len, created, modified) => {
                Some((path, *len, *created, *modified))
            }
            Msg::SizeSmaller(path, len, created, modified) => {
                Some((path, *len, *created, *modified))
            }
            Msg::CreatedChange(path, len, created, modified) => {
                Some((path, *len, *created, *modified))
            }
            Msg::Stop => None,
        }
    }

    pub fn is_truncated_or_rotated(&self) -> bool {
        match self {
            Msg::SizeSmaller(_path, _len, _created, _modified) => true,
            Msg::CreatedChange(_path, _len, _created, _modified) => true,
            Msg::ModifiedChange(_path, _len, _created, _modified) => false,
            Msg::Stop => false,
        }
    }
}

pub struct Handlers {
    // order matters, routes are tried from first to last and first that matches is used
    handlers: HashMap<String, FileState>,
    max_lines: u32,
    line_regex: Vec<LogRegex>,
    restore_state: RestoreState,
    content_sender: Sender<(Content, HandlerInfo)>,
}

#[derive(Debug, Clone)]
pub struct LogRegex {
    pub regex: Regex,
    pub keep_last_line: bool,
}

impl Handlers {
    pub fn new(
        restore_state: RestoreState,
        max_lines: u32,
        line_regex: Vec<LineRegex>,
        content_sender: Sender<(Content, HandlerInfo)>,
    ) -> Handlers {
        let line_regex: Vec<LogRegex> = line_regex
            .into_iter()
            .map(|x| {
                let regex = RegexBuilder::new(&x.match_val)
                    .dot_matches_new_line(true)
                    .multi_line(true)
                    .build()
                    .expect("wrong regex");
                LogRegex {
                    regex,
                    keep_last_line: x.keep_last_line,
                }
            })
            .collect();

        Handlers {
            restore_state,
            max_lines,
            line_regex,
            handlers: HashMap::new(),
            content_sender,
        }
    }

    pub fn get_handler(&mut self, path: &str) -> FileState {
        let (modified, created, offset, checksum) = match self.restore_state.get_handler_state(path)
        {
            Some(handler_info) => (
                handler_info.modified,
                handler_info.created,
                handler_info.offset,
                ContentChecksum::Restored {
                    hash: handler_info.hash.clone(),
                    offset: handler_info.hash_offset,
                    len: handler_info.hash_content_len,
                },
            ),
            None => (UNIX_EPOCH, UNIX_EPOCH, 0, ContentChecksum::None),
        };

        let mut file_state = FileState::new(
            path.to_string(),
            modified,
            created,
            checksum,
            self.content_sender.clone(),
        );

        if offset > 0 {
            info!(
                "restoring {} with offset: {}, modified: {:?}, created: {:?}",
                path, offset, modified, created
            );
        }
        file_state.seek(offset);
        file_state
    }

    pub fn handle(&mut self, msg: Msg) -> Result<(), Error<MsgInput>> {
        match msg.info() {
            Some((path, len, created, modified)) => {
                match self.handlers.get_mut(path) {
                    Some(file_state) => {
                        file_state.len = len;
                        file_state.modified = modified;
                        file_state.created = created;
                        if msg.is_truncated_or_rotated() {
                            file_state.mark_to_reset_on_eof();
                        }
                        file_state.read_some(self.max_lines, self.line_regex.clone())?;
                    }
                    None => {
                        let mut file_state = self.get_handler(&path);
                        file_state.len = len;
                        file_state.modified = modified;
                        file_state.created = created;
                        if msg.is_truncated_or_rotated() {
                            file_state.mark_to_reset_on_eof();
                        }
                        file_state.read_some(self.max_lines, self.line_regex.clone())?;
                        self.handlers.insert(path.to_string(), file_state);
                    }
                }

                Ok(())
            }
            None => {
                warn!("handling msg with no path: {:?}", msg);
                Ok(())
            }
        }
    }

    pub fn start(
        content_sender: Sender<(Content, HandlerInfo)>,
        source_receiver: Receiver<Msg>,
        recv_timeout: Duration,
        restore_state: RestoreState,
        max_lines: u32,
        line_regex: Vec<LineRegex>,
    ) -> JoinHandle<ProcessInfo> {
        thread::spawn(move || {
            let mut handlers = Handlers::new(restore_state, max_lines, line_regex, content_sender);
            // we check for has more only if there was a message before
            // or the last check had a handler which had more
            let mut check_for_has_more = false;
            loop {
                match source_receiver.recv_timeout(recv_timeout) {
                    Err(error) => {
                        if check_for_has_more && error.is_timeout() {
                            let mut at_least_one_has_more = false;
                            // check if there are handlers who have more to read
                            for (id, file_state) in handlers.handlers.iter_mut() {
                                if file_state.has_more {
                                    at_least_one_has_more = true;
                                    trace!("handler {} has more to read, reading", id);
                                    match file_state
                                        .read_some(handlers.max_lines, handlers.line_regex.clone())
                                    {
                                        Ok(_) => {}
                                        Err(err) => {
                                            error!("handler.read_some: {}", err);
                                        }
                                    }
                                } else {
                                    trace!("handler {} is up to date", id);
                                }
                            }

                            check_for_has_more = at_least_one_has_more;
                        } else if error.is_disconnected() {
                            info!("disconnected, stopping");
                            return handlers.stop();
                        }
                    }
                    Ok(Msg::Stop) => {
                        info!("stop received");
                        return handlers.stop();
                    }
                    Ok(msg) => {
                        check_for_has_more = true;
                        match handlers.handle(msg) {
                            Ok(_) => {}
                            Err(error) => error!("Handlers::loop: {:?}", error),
                        }
                    }
                }
            }
        })
    }

    fn stop(self) -> ProcessInfo {
        return ProcessInfo::HandlerInfo(
            self.handlers
                .iter()
                .map(|(_id, file_state)| file_state.to_info())
                .collect(),
        );
    }
}
