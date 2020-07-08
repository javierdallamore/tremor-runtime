#![allow(dead_code)]
use super::handlers;
use super::process::{HandlerInfo, ProcessInfo};
use super::restore::RestoreState;
use crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender};
use glob::Pattern;
use regex::{self, Regex};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use walkdir::{DirEntry, WalkDir};

#[derive(Debug, Clone)]
pub enum Matcher {
    Exact(String),
    Regex(regex::Regex),
    RegexSet(regex::RegexSet),
    Glob(Pattern),

    All(Vec<Matcher>),
    Any(Vec<Matcher>),
    None(Vec<Matcher>),
}

#[derive(Debug)]
pub struct MatchRules {
    rules: Vec<MatchRule>,
}

#[derive(Debug, Clone)]
pub enum RuleAction {
    Reject,
    Accept,
}

#[derive(Debug, Clone)]
pub struct MatchRule {
    matcher: Matcher,
    action: RuleAction,
}

#[derive(Debug)]
pub struct ChangeWatcher {
    paths: HashMap<String, ChangeState>,
    inactive_paths: HashMap<String, ChangeState>,
}

#[derive(Debug, Clone)]
struct ChangeState {
    modified: SystemTime,
    created: SystemTime,
    len: u64,
}

pub enum Msg {
    Path(String),
    Stop,
}

impl MatchRules {
    pub fn new(rules: Vec<MatchRule>) -> MatchRules {
        MatchRules { rules }
    }

    pub fn matches(&self, s: &str) -> bool {
        for rule in self.rules.iter() {
            match rule.check(s) {
                Some(RuleAction::Accept) => {
                    return true;
                }
                Some(RuleAction::Reject) => {
                    return false;
                }
                None => {}
            }
        }

        return false;
    }

    pub fn matches_dir_entry(&self, de: &DirEntry) -> bool {
        self.matches(&de.path().to_string_lossy())
    }
}

impl MatchRule {
    pub fn new(matcher: Matcher, action: RuleAction) -> MatchRule {
        MatchRule { matcher, action }
    }

    pub fn accept(matcher: Matcher) -> MatchRule {
        MatchRule::new(matcher, RuleAction::Accept)
    }

    pub fn reject(matcher: Matcher) -> MatchRule {
        MatchRule::new(matcher, RuleAction::Reject)
    }

    pub fn check(&self, s: &str) -> Option<RuleAction> {
        if self.matcher.matches(s) {
            Some(self.action.clone())
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct ParseError {
    description: String,
}

impl ParseError {
    pub fn from_error(err: &dyn Error) -> ParseError {
        ParseError {
            description: err.to_string(),
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Parse Error: {}", self.description)
    }
}

impl Matcher {
    pub fn exact(s: String) -> Matcher {
        Matcher::Exact(s)
    }

    pub fn regex(re: regex::Regex) -> Matcher {
        Matcher::Regex(re)
    }

    pub fn regex_str(s: &str) -> Result<Matcher, ParseError> {
        match Regex::new(s) {
            Ok(re) => Ok(Matcher::regex(re)),
            Err(error) => Err(ParseError::from_error(&error)),
        }
    }

    pub fn regex_set(re_set: regex::RegexSet) -> Matcher {
        Matcher::RegexSet(re_set)
    }

    pub fn glob(pattern: Pattern) -> Matcher {
        Matcher::Glob(pattern)
    }

    pub fn glob_str(s: &str) -> Result<Matcher, ParseError> {
        match Pattern::new(s) {
            Ok(pattern) => Ok(Matcher::glob(pattern)),
            Err(error) => Err(ParseError::from_error(&error)),
        }
    }

    pub fn all(ms: Vec<Matcher>) -> Matcher {
        Matcher::All(ms)
    }

    pub fn any(ms: Vec<Matcher>) -> Matcher {
        Matcher::Any(ms)
    }

    pub fn none(ms: Vec<Matcher>) -> Matcher {
        Matcher::None(ms)
    }

    pub fn matches(&self, s: &str) -> bool {
        match self {
            Matcher::Exact(v) => v == s,
            Matcher::Regex(re) => re.is_match(s),
            Matcher::RegexSet(re_set) => re_set.is_match(s),
            Matcher::Glob(p) => p.matches(s),
            Matcher::All(ms) => ms.iter().all(|m| m.matches(s)),
            Matcher::Any(ms) => ms.iter().any(|m| m.matches(s)),
            Matcher::None(ms) => !ms.iter().any(|m| m.matches(s)),
        }
    }

    pub fn matches_path(&self, p: &Path) -> bool {
        self.matches(&p.to_string_lossy())
    }

    pub fn matches_dir_entry(&self, de: &DirEntry) -> bool {
        self.matches(&de.path().to_string_lossy())
    }

    pub fn to_accept_rule(self) -> MatchRule {
        MatchRule::accept(self)
    }

    pub fn to_reject_rule(self) -> MatchRule {
        MatchRule::reject(self)
    }
}

impl std::fmt::Display for Matcher {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Matcher::Exact(v) => write!(f, "Matcher::Exact({})", v),
            Matcher::Regex(re) => write!(f, "Matcher::Regex({})", re),
            Matcher::RegexSet(re_set) => write!(f, "Matcher::RegexSet({:?})", re_set),
            Matcher::Glob(p) => write!(f, "Matcher::Glob({:?})", p),
            Matcher::All(ms) => write!(f, "Matcher::All({:?})", ms),
            Matcher::Any(ms) => write!(f, "Matcher::Any({:?})", ms),
            Matcher::None(ms) => write!(f, "Matcher::None({:?})", ms),
        }
    }
}

impl ChangeWatcher {
    pub fn new() -> ChangeWatcher {
        ChangeWatcher {
            paths: HashMap::new(),
            inactive_paths: HashMap::new(),
        }
    }

    pub fn from_restore_state(restore_state: &RestoreState) -> ChangeWatcher {
        let mut change_watcher = ChangeWatcher::new();

        for (path, handler_info) in restore_state.handler_infos.iter() {
            change_watcher
                .paths
                .insert(path.clone(), ChangeState::from_handler_info(handler_info));
        }

        change_watcher
    }

    pub fn add_path(&mut self, path: &str) {
        if !self.paths.contains_key(path) {
            match self.inactive_paths.remove(path) {
                Some(change_state) => {
                    debug!("add_path {} (inactive)", path);
                    self.paths.insert(path.to_string(), change_state);
                }
                None => {
                    debug!("add_path {} (new)", path);
                    self.paths.insert(path.to_string(), ChangeState::new());
                }
            }
        } else {
            debug!("add_path {} (exists)", path);
        }
    }

    pub fn check(&mut self, s: &Sender<handlers::Msg>) {
        for (path, change_state) in self.paths.iter_mut() {
            trace!("check {}", path);

            match change_state.check(path) {
                Ok(Some(msg)) => match s.send(msg) {
                    Ok(_) => {}
                    Err(error) => {
                        error!("error sending change: {}", error);
                    }
                },
                Ok(None) => {}
                Err(err) => {
                    warn!("error checking file {}: {}", path, err);
                }
            }
        }
    }

    pub fn evict_inactive(&mut self, older_than: Duration) {
        let len_before = self.paths.len();
        let mut new_paths = HashMap::new();
        for (path, change_state) in self.paths.drain() {
            match change_state.modified.elapsed() {
                Ok(elapsed) => {
                    let retain = elapsed < older_than;
                    trace!("retain {}? {}", path, retain);
                    if retain {
                        new_paths.insert(path, change_state);
                    } else {
                        self.inactive_paths.insert(path, change_state);
                    }
                }
                Err(error) => {
                    warn!(
                        "Error getting modification ellapsed time for file {}: {}",
                        path, error
                    );

                    self.inactive_paths.insert(path, change_state);
                }
            }
        }

        self.paths = new_paths;

        debug!(
            "evict inactive ({} items before, {} after)",
            len_before,
            self.paths.len()
        );
    }

    pub fn start(
        mut self,
        r: Receiver<Msg>,
        s: Sender<handlers::Msg>,
        recv_timeout: Duration,
        check_inverval: Duration,
        eviction_interval: Duration,
        evict_older_than: Duration,
    ) -> JoinHandle<ProcessInfo> {
        thread::spawn(move || {
            let mut last_check: Option<Instant> = None;
            let mut last_eviction: Option<Instant> = None;
            loop {
                match r.recv_timeout(recv_timeout) {
                    Ok(Msg::Path(path)) => {
                        self.add_path(&path);
                    }
                    Ok(Msg::Stop) => {
                        info!("stop received");
                        return ProcessInfo::Nothing;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        trace!("recv timeout");
                    }
                    Err(error) => {
                        error!("{}", error);
                    }
                }

                if last_check.is_none() || last_check.unwrap().elapsed() > check_inverval {
                    last_check = Some(Instant::now());
                    self.check(&s);
                    debug!("check in {:?}", last_check.unwrap().elapsed());
                }

                if last_eviction.is_none() || last_eviction.unwrap().elapsed() > eviction_interval {
                    last_eviction = Some(Instant::now());
                    self.evict_inactive(evict_older_than);
                    debug!("evict in {:?}", last_eviction.unwrap().elapsed());
                }
            }
        })
    }
}

impl ChangeState {
    fn new() -> ChangeState {
        ChangeState {
            modified: UNIX_EPOCH,
            created: UNIX_EPOCH,
            len: 0,
        }
    }

    fn from_handler_info(handler_info: &HandlerInfo) -> ChangeState {
        ChangeState {
            modified: handler_info.modified,
            created: handler_info.created,
            len: handler_info.len,
        }
    }

    fn check(&mut self, path: &str) -> io::Result<Option<handlers::Msg>> {
        let metadata = fs::metadata(path)?;
        let created = match metadata.created() {
            Ok(v) => v,
            // trace since this fails on linux
            Err(err) => {
                if (len == 0) {
                    error!("error getting created from metadata {}", err);
                }
                self.created
            }
        };

        let len = metadata.len();
        let modified = metadata.modified()?;

        let current_created = self.created;
        let current_len = self.len;
        let current_modified = self.modified;

        self.created = created;
        self.len = len;
        self.modified = modified;

        if created > current_created {
            Ok(Some(handlers::Msg::CreatedChange(
                path.to_string(),
                len,
                created,
                modified,
            )))
        } else if len < current_len {
            Ok(Some(handlers::Msg::SizeSmaller(
                path.to_string(),
                len,
                created,
                modified,
            )))
        } else if modified > current_modified {
            Ok(Some(handlers::Msg::ModifiedChange(
                path.to_string(),
                len,
                created,
                modified,
            )))
        } else {
            Ok(None)
        }
    }
}

pub fn walkdir(base_path: &str) -> impl Iterator<Item = DirEntry> {
    WalkDir::new(base_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|de| de.path().is_file())
}

pub fn walkdir_filter_send(
    base_path: &str,
    m: &MatchRules,
    out: &Sender<Msg>,
) -> Result<(), SendError<Msg>> {
    for dir_entry in walkdir(base_path).filter(|de| {
        let matches = m.matches_dir_entry(de);
        trace!("walkdir {:?} matches {:?}? {}", de, m, matches);
        matches
    }) {
        out.send(Msg::Path(dir_entry.path().to_string_lossy().to_string()))?;
    }

    Ok(())
}

pub enum WalkerMsg {
    Stop,
}

pub fn walkdir_filter_send_thread(
    base_path: &str,
    m: MatchRules,
    out: Sender<Msg>,
    msg_r: Receiver<WalkerMsg>,
    sleep_duration: Duration,
) -> JoinHandle<ProcessInfo> {
    let my_base_path = base_path.to_string();
    thread::spawn(move || loop {
        let start = Instant::now();
        match walkdir_filter_send(&my_base_path, &m, &out) {
            Ok(_) => {}
            Err(error) => error!("walkdir_filter_send({}): {}", my_base_path, error),
        }

        debug!("walkdir {} in {:?}", my_base_path, start.elapsed());

        match msg_r.recv_timeout(sleep_duration) {
            Ok(WalkerMsg::Stop) => {
                info!("stop received");
                match out.send(Msg::Stop) {
                    Ok(_) => {}
                    Err(err) => {
                        error!("Sending stop to output: {}", err);
                    }
                }
                return ProcessInfo::Nothing;
            }
            Err(RecvTimeoutError::Timeout) => {
                trace!("recv timeout");
            }
            Err(error) => {
                error!("{}", error);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_exact() {
        let m = Matcher::exact("foo".to_string());

        assert_eq!(m.matches("foo"), true);
        assert_eq!(m.matches(" foo"), false);
        assert_eq!(m.matches("Foo"), false);
        assert_eq!(m.matches("FOO"), false);
    }

    #[test]
    fn match_regex() {
        let m = Matcher::regex(regex::Regex::new("^fo+$").unwrap());
        assert_eq!(m.matches("fo"), true);
        assert_eq!(m.matches("foo"), true);
        assert_eq!(m.matches("fooo"), true);
        assert_eq!(m.matches(" foo"), false);
        assert_eq!(m.matches("Foo"), false);
        assert_eq!(m.matches("FOO"), false);
    }

    #[test]
    fn match_regex_set() {
        let m = Matcher::regex_set(regex::RegexSet::new(&[r"^fo+$", r"^ba+r$"]).unwrap());
        assert_eq!(m.matches("fo"), true);
        assert_eq!(m.matches("foo"), true);
        assert_eq!(m.matches("fooo"), true);
        assert_eq!(m.matches(" foo"), false);
        assert_eq!(m.matches("Foo"), false);
        assert_eq!(m.matches("FOO"), false);

        assert_eq!(m.matches("bar"), true);
        assert_eq!(m.matches("baar"), true);
        assert_eq!(m.matches("baaar"), true);
        assert_eq!(m.matches(" bar"), false);
        assert_eq!(m.matches("Bar"), false);
        assert_eq!(m.matches("BAR"), false);
    }

    #[test]
    fn match_glob() {
        let m = Matcher::glob(Pattern::new("a/*/c").unwrap());
        assert_eq!(m.matches("a/b/c"), true);
        assert_eq!(m.matches("a/bbbbb/c"), true);
        assert_eq!(m.matches("a/a/c"), true);
        assert_eq!(m.matches("a/c"), false);
    }

    #[test]
    fn match_all() {
        let m = Matcher::all(vec![
            Matcher::regex(regex::Regex::new("^fo+$").unwrap()),
            Matcher::exact("foo".to_string()),
        ]);

        assert_eq!(m.matches("foo"), true);
        assert_eq!(m.matches("fo"), false);
        assert_eq!(m.matches("fooo"), false);
    }

    #[test]
    fn match_any() {
        let m = Matcher::any(vec![
            Matcher::regex(regex::Regex::new("^fo+$").unwrap()),
            Matcher::exact("foo".to_string()),
        ]);

        assert_eq!(m.matches("foo"), true);
        assert_eq!(m.matches("fo"), true);
        assert_eq!(m.matches("fooo"), true);
    }

    #[test]
    fn match_none() {
        let m = Matcher::none(vec![
            Matcher::regex(regex::Regex::new("^fo+$").unwrap()),
            Matcher::exact("foo".to_string()),
        ]);

        assert_eq!(m.matches("foo"), false);
        assert_eq!(m.matches("fo"), false);
        assert_eq!(m.matches("fooo"), false);
        assert_eq!(m.matches("Foo"), true);
    }

    #[test]
    fn match_rules_one_accept() {
        let m = MatchRules::new(vec![Matcher::exact("foo".to_string()).to_accept_rule()]);

        assert_eq!(m.matches("foo"), true);
        assert_eq!(m.matches("fo"), false);
        assert_eq!(m.matches("fooo"), false);
    }

    #[test]
    fn match_rules_one_reject() {
        let m = MatchRules::new(vec![Matcher::exact("foo".to_string()).to_reject_rule()]);

        assert_eq!(m.matches("foo"), false);
        assert_eq!(m.matches("fo"), false);
        assert_eq!(m.matches("fooo"), false);
    }

    #[test]
    fn match_rules_contradiction_accept() {
        let m = MatchRules::new(vec![
            Matcher::exact("foo".to_string()).to_accept_rule(),
            Matcher::exact("foo".to_string()).to_reject_rule(),
        ]);

        assert_eq!(m.matches("foo"), true);
        assert_eq!(m.matches("fo"), false);
        assert_eq!(m.matches("fooo"), false);
    }

    #[test]
    fn match_rules_contradiction_reject() {
        let m = MatchRules::new(vec![
            Matcher::exact("foo".to_string()).to_reject_rule(),
            Matcher::exact("foo".to_string()).to_accept_rule(),
        ]);

        assert_eq!(m.matches("foo"), false);
        assert_eq!(m.matches("fo"), false);
        assert_eq!(m.matches("fooo"), false);
    }
}
