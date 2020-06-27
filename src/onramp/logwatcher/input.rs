#![allow(dead_code)]
use super::handlers::LogRegex;
use super::process::{hash_str, HandlerInfo};
pub(crate) use crate::utils::{hostname, nanotime};
use crossbeam_channel::{SendError, Sender};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};

pub type Output = HashMap<String, String>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Content {
    pub hostname: String,
    pub file_path: String,
    pub file_modified_ts: u64,
    pub file_created_ts: u64,
    pub file_offset: u64,
    pub line_consumed_ts: u64,
    pub line: String,
    pub id: String,
}

#[derive(Debug)]
pub enum MsgInput {
    Stop,
    Data(Output),
}

#[derive(Debug)]
pub enum Error<T> {
    Send(SendError<T>),
    IO(io::Error),
    Regex(regex::Error),
}

impl From<io::Error> for Error<MsgInput> {
    fn from(t: io::Error) -> Error<MsgInput> {
        Error::IO(t)
    }
}

impl From<SendError<MsgInput>> for Error<MsgInput> {
    fn from(t: SendError<MsgInput>) -> Error<MsgInput> {
        Error::Send(t)
    }
}

impl From<regex::Error> for Error<MsgInput> {
    fn from(t: regex::Error) -> Error<MsgInput> {
        Error::Regex(t)
    }
}

pub struct FileState {
    path: String,
    pub modified: SystemTime,
    pub created: SystemTime,
    pub len: u64,
    offset: u64,
    buffer: String,
    checksum: ContentChecksum,
    reader: Option<BufReader<File>>,
    pub has_more: bool,
    // when a file is rotated we need to read until the end and then reset the
    // state and reopen to start reading from the new one
    reset_on_eof: bool,
    file_lines: Vec<String>,
    content_sender: Sender<(Content, HandlerInfo)>,
}

#[derive(Debug)]
pub enum ContentChecksum {
    Restored {
        hash: String,
        offset: u64,
        len: usize,
    },
    Read {
        content: String,
        offset: u64,
    },
    None,
}

impl ContentChecksum {
    // offset of the next line to read (the one after the one we read)
    fn offset_next(&self) -> u64 {
        match self {
            ContentChecksum::Restored { offset, len, .. } => offset + (len + 1) as u64,
            ContentChecksum::Read { offset, content } => offset + (content.len() + 1) as u64,
            ContentChecksum::None => 0,
        }
    }

    fn offset(&self) -> u64 {
        match self {
            ContentChecksum::Restored { offset, .. } => *offset,
            ContentChecksum::Read { offset, .. } => *offset,
            ContentChecksum::None => 0,
        }
    }

    fn len(&self) -> usize {
        match self {
            ContentChecksum::Restored { len, .. } => *len,
            ContentChecksum::Read { content, .. } => content.len(),
            ContentChecksum::None => 0,
        }
    }

    fn hash(&self) -> String {
        match self {
            ContentChecksum::Restored { hash, .. } => hash.clone(),
            ContentChecksum::Read { content, .. } => hash_str(content),
            ContentChecksum::None => String::from(""),
        }
    }
}

impl FileState {
    pub fn new(
        path: String,
        modified: SystemTime,
        created: SystemTime,
        checksum: ContentChecksum,
        content_sender: Sender<(Content, HandlerInfo)>,
    ) -> FileState {
        FileState {
            path,
            modified,
            created,
            checksum,
            len: 0,
            offset: 0,
            buffer: String::from(""),
            reader: None,
            has_more: false,
            reset_on_eof: false,
            file_lines: Vec::new(),
            content_sender,
        }
    }

    pub fn seek(&mut self, offset: u64) {
        self.offset = offset;
    }

    pub fn reset(&mut self) {
        self.offset = 0;
        self.buffer = String::from("");
        self.reader = None;
        self.has_more = false;
        self.reset_on_eof = false;
    }

    pub fn mark_to_reset_on_eof(&mut self) {
        self.reset_on_eof = true;
    }

    pub fn read_some(
        &mut self,
        max_lines: u32,
        line_regex: Vec<LogRegex>,
    ) -> Result<bool, SendError<MsgInput>> {
        let mut line_count = 0;
        let mut has_more = true;
        while has_more && line_count < max_lines {
            match self.read_line(max_lines as usize, line_regex.clone()) {
                Ok(Some(line)) if line.is_empty() => {
                    has_more = true;
                    line_count += 1;
                }
                Ok(Some(line)) => {
                    let now = (nanotime() / 1_000_000) as u64;

                    let file_modified_ts = self
                        .modified
                        .duration_since(UNIX_EPOCH)
                        .expect("time went backwards")
                        .as_millis() as u64;
                    let file_created_ts = self
                        .created
                        .duration_since(UNIX_EPOCH)
                        .expect("time went backwards")
                        .as_millis() as u64;

                    let id_str = format!(
                        "{}{}{}{}{}",
                        file_created_ts,
                        hostname(),
                        self.path,
                        self.offset,
                        line
                    );

                    let content = Content {
                        hostname: hostname(),
                        file_path: self.path.clone(),
                        file_modified_ts: file_modified_ts,
                        file_created_ts: file_created_ts,
                        file_offset: self.offset,
                        line_consumed_ts: now,
                        line: line,
                        id: hash_str(&id_str),
                    };

                    has_more = true;
                    line_count += 1;
                    match self.content_sender.send((content, self.to_info())) {
                        Ok(_) => {}
                        Err(error) => {
                            error!("error sending content: {}", error);
                        }
                    }
                }
                Ok(None) => {
                    if self.reset_on_eof {
                        self.reset();
                    }
                    has_more = false;
                }
                Err(_) => has_more = false,
            }
        }

        self.has_more = has_more;
        Ok(has_more)
    }

    fn read_line(
        &mut self,
        max_lines: usize,
        line_regex: Vec<LogRegex>,
    ) -> Result<Option<String>, io::Error> {
        match self.reader {
            Some(ref mut reader) => match reader.read_line(&mut self.buffer) {
                Ok(size) => {
                    if size == 0 {
                        Ok(None)
                    } else {
                        self.offset += size as u64;
                        if self.buffer.ends_with("\n") {
                            self.buffer.pop();
                            let content = self.buffer.clone();

                            let content_offset = self.offset - (content.len() + 1) as u64;
                            self.checksum = ContentChecksum::Read {
                                content,
                                offset: content_offset,
                            };

                            if line_regex.is_empty() {
                                let result = self.buffer.clone();
                                self.buffer.clear();
                                Ok(Some(result))
                            } else {
                                //TODO find a better way for multiple lines accumulator
                                let mut result = String::from("");
                                let mut keep_last_line = false;
                                self.file_lines.push(self.buffer.clone());
                                let lines_joined = self.file_lines.join("\n");
                                'outer: for regex in &line_regex {
                                    for mat in regex.regex.find_iter(&lines_joined) {
                                        keep_last_line = regex.keep_last_line;
                                        result = lines_joined[mat.start()..mat.end()].to_string();
                                        break 'outer;
                                    }
                                }

                                if !result.is_empty() {
                                    self.file_lines.clear();
                                    if keep_last_line {
                                        self.file_lines = vec![self.buffer.clone()];
                                    }
                                }

                                // remove lines while file_lines > max_lines
                                while self.file_lines.len() > max_lines {
                                    self.file_lines.remove(0);
                                }

                                self.buffer.clear();
                                Ok(Some(result))
                            }
                        } else {
                            Ok(None)
                        }
                    }
                }
                Err(error) => Err(error),
            },
            None => {
                let f = File::open(self.path.to_string())?;
                let mut bf = BufReader::new(f);
                match bf.seek(SeekFrom::Start(self.offset)) {
                    Ok(_) => {}
                    Err(err) => {
                        warn!(
                            "Error trying to seek {} to {}, trying to start from bof: {}",
                            self.path, self.offset, err
                        );
                        self.offset = 0;
                        bf.seek(SeekFrom::Start(0))?;
                    }
                }
                self.reader = Some(bf);
                self.read_line(max_lines, line_regex)
            }
        }
    }

    pub fn to_info(&self) -> HandlerInfo {
        let offset = self.checksum.offset_next();
        HandlerInfo::new(
            self.len,
            offset,
            self.modified,
            self.created,
            self.checksum.hash(),
            self.checksum.offset(),
            self.checksum.len(),
            self.path.clone(),
        )
    }
}
