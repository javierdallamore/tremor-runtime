use regex::Regex;
use sha2::{self, Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub enum ProcessInfo {
    Nothing,
    HandlerInfo(Vec<HandlerInfo>),
}

#[derive(Debug)]
pub struct HandlerInfo {
    pub path: String,
    // hash calculated by hashing hash_content_len bytes from hash_offset
    pub hash: String,
    // start offset of the content we used to hash
    pub hash_offset: u64,
    // how much do we have to read from hash_offset to get the content to calculate the has
    pub hash_content_len: usize,
    // file size on last notification
    pub len: u64,
    // offset where we finished reading a complete line
    pub offset: u64,
    // modified time on last notification
    pub modified: SystemTime,
    pub created: SystemTime,
}

impl HandlerInfo {
    pub fn new(
        len: u64,
        offset: u64,
        modified: SystemTime,
        created: SystemTime,
        hash: String,
        hash_offset: u64,
        hash_content_len: usize,
        path: String,
    ) -> HandlerInfo {
        HandlerInfo {
            len,
            offset,
            modified,
            created,
            hash,
            hash_offset,
            hash_content_len,
            path,
        }
    }

    pub fn read_len_at<T: Read + Seek>(
        f: &mut T,
        offset: u64,
        len: usize,
    ) -> io::Result<Option<Vec<u8>>> {
        match f.seek(SeekFrom::Start(offset)) {
            Ok(real_seek) => {
                if real_seek != offset {
                    debug!("seek result ({}) != offset ({})", real_seek, offset);
                    return Ok(None);
                }
                let mut buffer = vec![0u8; len];
                f.read_exact(&mut buffer)?;

                Ok(Some(buffer))
            }
            Err(err) => Err(err),
        }
    }

    pub fn hash_read<T: Read + Seek>(
        f: &mut T,
        offset: u64,
        len: usize,
    ) -> io::Result<Option<String>> {
        match HandlerInfo::read_len_at(f, offset, len) {
            Ok(Some(buffer)) => {
                let hash = hash_bytes(&buffer);
                debug!(
                    "hashing {:?} to {} / {}",
                    std::str::from_utf8(buffer.as_slice()),
                    hash,
                    hash_str(std::str::from_utf8(buffer.as_slice()).unwrap())
                );
                Ok(Some(hash))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn is_same_hash(&self) -> io::Result<bool> {
        let mut f = File::open(self.path.clone())?;
        match HandlerInfo::hash_read(&mut f, self.hash_offset, self.hash_content_len) {
            Ok(Some(current_hash)) => {
                let is_same = current_hash == self.hash;
                debug!(
                    "{}: {} {} == {}",
                    self.path, is_same, current_hash, self.hash
                );

                Ok(is_same)
            }
            Ok(None) => Ok(false),
            Err(err) => {
                debug!("error: {} {:?}", err, self);
                Err(err)
            }
        }
    }

    pub fn to_line(&self) -> Option<String> {
        if self.hash_content_len == 0 {
            // don't serialize info that doesn't have a hash
            None
        } else {
            let modified_millis_since_epoch = self
                .modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let created_millis_since_epoch = self
                .created
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            Some(format!(
                "HandlerInfo:1:{}:{}:{}:{}:{}:{}:{}:{}\n",
                self.len,
                self.offset,
                modified_millis_since_epoch,
                created_millis_since_epoch,
                self.hash,
                self.hash_offset,
                self.hash_content_len,
                self.path
            ))
        }
    }
}

pub enum ProcessInfoItem {
    HandlerInfo(HandlerInfo),
    Unk(String),
}

impl ProcessInfoItem {
    pub fn from_line(line: &str) -> ProcessInfoItem {
        lazy_static! {
            // HandlerInfo:1:4365720:1121084:1559297482356:1559297482356:008D22718ED48AF288E21873F1233668DC88361109BA4F450A916245486CF6F4:1121084:295:access.log
            static ref RE_HANDLE_INFO_1: Regex = Regex::new("^HandlerInfo:1:([0-9]+):([0-9]+):([0-9]+):([0-9]+):([0-9A-Z]+):([0-9]+):([0-9]+):(.+)\n?$").unwrap();
        }

        if line.starts_with("HandlerInfo:1:") {
            match RE_HANDLE_INFO_1.captures(line) {
                Some(captures) => {
                    // since they match they are available and have the right format
                    let len_str = captures.get(1).unwrap().as_str();
                    let offset_str = captures.get(2).unwrap().as_str();
                    let modified_millis_since_epoch_str = captures.get(3).unwrap().as_str();
                    let created_millis_since_epoch_str = captures.get(4).unwrap().as_str();
                    let hash = captures.get(5).unwrap().as_str();
                    let hash_offset_str = captures.get(6).unwrap().as_str();
                    let hash_content_len_str = captures.get(7).unwrap().as_str();
                    let path = captures.get(8).unwrap().as_str();

                    let len = len_str.parse::<u64>().unwrap();
                    let offset = offset_str.parse::<u64>().unwrap();
                    let modified_millis_since_epoch =
                        modified_millis_since_epoch_str.parse::<u64>().unwrap();
                    let modified = UNIX_EPOCH
                        .checked_add(Duration::from_millis(modified_millis_since_epoch))
                        .unwrap();
                    let created_millis_since_epoch =
                        created_millis_since_epoch_str.parse::<u64>().unwrap();
                    let created = UNIX_EPOCH
                        .checked_add(Duration::from_millis(created_millis_since_epoch))
                        .unwrap();
                    let hash_offset = hash_offset_str.parse::<u64>().unwrap();
                    let hash_content_len = hash_content_len_str.parse::<usize>().unwrap();

                    let handler_info = HandlerInfo::new(
                        len,
                        offset,
                        modified,
                        created,
                        hash.to_string(),
                        hash_offset,
                        hash_content_len,
                        path.to_string(),
                    );
                    debug!("parsed {:?}", handler_info);
                    return ProcessInfoItem::HandlerInfo(handler_info);
                }
                None => {}
            }
        }

        ProcessInfoItem::Unk(line.to_string())
    }
}

impl Clone for HandlerInfo {
    fn clone(&self) -> HandlerInfo {
        HandlerInfo {
            path: self.path.clone(),
            hash: self.hash.clone(),
            hash_offset: self.hash_offset.clone(),
            hash_content_len: self.hash_content_len.clone(),
            len: self.len.clone(),
            offset: self.offset.clone(),
            modified: self.modified.clone(),
            created: self.created.clone(),
        }
    }
}

fn hash_bytes(v: &Vec<u8>) -> String {
    let mut sha256 = Sha256::new();
    sha256.input(v);
    String::from(format!("{:X}", sha256.result()))
}

pub fn hash_str(s: &str) -> String {
    hash_bytes(&s.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn same_hash() {
        let content =
            "one two three four five six\nseven eight nine ten\neleven!!1! 12 13 14 15 16";
        let mut buff = Cursor::new(content.as_bytes());
        let hash_offset = 10;
        let hash_len = 10;
        let hash_offset_to = hash_offset + hash_len;
        let hash_content = &content[hash_offset..hash_offset_to];

        let read_result = HandlerInfo::read_len_at(&mut buff, hash_offset as u64, hash_len)
            .unwrap()
            .unwrap();

        let content_from_buffer = std::str::from_utf8(read_result.as_slice()).unwrap();
        println!("'{}' == '{}'?", hash_content, content_from_buffer);
        assert_eq!(hash_content, content_from_buffer);
        let hash_from_content = hash_str(hash_content);
        let hash_from_buffer = HandlerInfo::hash_read(&mut buff, hash_offset as u64, hash_len)
            .unwrap()
            .unwrap();
        println!("{} == {}?", hash_from_content, hash_from_buffer);
        assert_eq!(hash_from_content, hash_from_buffer);
        println!(
            "{} == {}?",
            hash_from_content,
            hash_str(content_from_buffer)
        );
        assert_eq!(hash_from_content, hash_str(content_from_buffer));
    }
}
