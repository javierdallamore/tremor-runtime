use super::process::{HandlerInfo, ProcessInfoItem};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};


#[derive(Debug)]
pub struct RestoreState {
    pub handler_infos: HashMap<String, HandlerInfo>,
}

#[derive(Debug)]
pub enum Error {
    NotFound(String),
}

impl RestoreState {
    pub fn empty() -> RestoreState {
        RestoreState {
            handler_infos: HashMap::new(),
        }
    }

    pub fn from_file(path: &str) -> Result<RestoreState, Error> {
        let mut handler_infos = HashMap::new();
        match File::open(path) {
            Ok(file) => {
                let reader = BufReader::new(file);

                for line_r in reader.lines() {
                    match line_r {
                        Ok(line) => match ProcessInfoItem::from_line(&line) {
                            ProcessInfoItem::HandlerInfo(handler_info) => {
                                handler_infos.insert(handler_info.path.clone(), handler_info);
                            }
                            ProcessInfoItem::Unk(unk_line) => {
                                debug!("Unknown RestoreState line: {}", unk_line);
                            }
                        },
                        Err(err) => {
                            error!("reading line in {}: {}", path, err);
                        }
                    }
                }

                Ok(RestoreState { handler_infos })
            }
            Err(err) => {
                info!("RestoreState::from_file({}): {}", path, err);
                Err(Error::NotFound(path.to_string()))
            }
        }
    }

    pub fn get_handler_state(&self, path: &str) -> Option<&HandlerInfo> {
        match self.handler_infos.get(path) {
            Some(handler_info) => match handler_info.is_same_hash() {
                Ok(true) => {
                    info!("restore state matches hash {:?}", handler_info);
                    Some(handler_info)
                }
                Ok(false) => {
                    info!("restore state doesn't match hash {:?}", handler_info);
                    None
                }
                Err(err) => {
                    warn!("error checking hash for restore state {}: {}", path, err);
                    None
                }
            },
            None => {
                info!("no restore state for path {}", path);
                None
            }
        }
    }
}

impl Clone for RestoreState {
    fn clone(&self) -> RestoreState {
        RestoreState {
            handler_infos: self.handler_infos.clone(),
        }
    }
}
