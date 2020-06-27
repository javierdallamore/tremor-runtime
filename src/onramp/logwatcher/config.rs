use super::path::{MatchRule, Matcher, ParseError, RuleAction};
use crate::dflt;
use crate::ramp;
use std::time::Duration;

use crate::utils::ConfigImpl;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// source logwatcher to read data from, it will be iterated over repeatedly,
    pub source: SourceConfig,
    #[serde(default = "dflt::d_false")]
    pub close_on_done: bool,
    pub restore_file: ramp::Config,
    pub throttle: u64,
}

impl ConfigImpl for Config {}

#[derive(Deserialize, Debug)]
pub struct SourceConfig {
    pub id: String,
    pub path: String,
    pub max_lines: Option<u32>,
    #[serde(rename = "regex")]
    pub line_regex: Option<Vec<LineRegex>>,
    #[serde(rename = "rule")]
    pub rules: Vec<RuleConfig>,
    #[serde(with = "serde_humanize_rs")]
    pub walk_interval: Option<Duration>,
    #[serde(with = "serde_humanize_rs")]
    pub receive_timeout: Option<Duration>,
    #[serde(with = "serde_humanize_rs")]
    pub check_interval: Option<Duration>,
    #[serde(with = "serde_humanize_rs")]
    pub eviction_interval: Option<Duration>,
    #[serde(with = "serde_humanize_rs")]
    pub evict_older_than: Option<Duration>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RuleConfig {
    pub action: ActionType,
    #[serde(rename = "type")]
    pub match_type: MatchType,
    #[serde(rename = "match")]
    pub match_val: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LineRegex {
    #[serde(rename = "match")]
    pub match_val: String,
    pub keep_last_line: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub enum ActionType {
    Accept,
    Reject,
}

#[derive(Deserialize, Debug, Clone)]
pub enum MatchType {
    Exact,
    Regex,
    Glob,
}

impl SourceConfig {
    pub fn to_source_spec(&self) -> Result<SourceSpec, ParseError> {
        let mut match_rules = vec![];
        for rule in self.rules.iter() {
            match_rules.push(rule.to_match_rule()?);
        }

        Ok(SourceSpec {
            id: self.id.clone(),
            path: self.path.clone(),
            max_lines: self.max_lines.unwrap_or(200),
            line_regex: self.line_regex.clone().unwrap_or(vec![]),
            rules: match_rules,
            walk_interval: self.walk_interval.unwrap_or(Duration::from_secs(60)),
            receive_timeout: self.receive_timeout.unwrap_or(Duration::from_millis(100)),
            check_interval: self.check_interval.unwrap_or(Duration::from_millis(100)),
            eviction_interval: self.eviction_interval.unwrap_or(Duration::from_secs(60)),
            evict_older_than: self.evict_older_than.unwrap_or(Duration::from_secs(180)),
        })
    }
}

impl RuleConfig {
    pub fn to_match_rule(&self) -> Result<MatchRule, ParseError> {
        let rule_action = self.action.to_rule_action();
        let matcher = match self.match_type {
            MatchType::Regex => Matcher::regex_str(&self.match_val)?,
            MatchType::Exact => Matcher::exact(self.match_val.to_string()),
            MatchType::Glob => Matcher::glob_str(&self.match_val)?,
        };

        Ok(MatchRule::new(matcher, rule_action))
    }
}

impl ActionType {
    pub fn to_rule_action(&self) -> RuleAction {
        match self {
            ActionType::Accept => RuleAction::Accept,
            ActionType::Reject => RuleAction::Reject,
        }
    }
}

impl Clone for SourceConfig {
    fn clone(&self) -> SourceConfig {
        let line_regex = match &self.line_regex {
            Some(l) => l.iter().map(|regex| regex.clone()).collect(),
            _ => vec![],
        };
        SourceConfig {
            id: self.id.clone(),
            path: self.path.clone(),
            max_lines: self.max_lines,
            line_regex: Some(line_regex),
            rules: self.rules.iter().map(|rule| rule.clone()).collect(),
            walk_interval: self.walk_interval.clone(),
            receive_timeout: self.receive_timeout.clone(),
            check_interval: self.check_interval.clone(),
            eviction_interval: self.eviction_interval.clone(),
            evict_older_than: self.evict_older_than.clone(),
        }
    }
}

#[derive(Debug)]
pub struct SourceSpec {
    pub id: String,
    pub path: String,
    pub max_lines: u32,
    pub line_regex: Vec<LineRegex>,
    pub rules: Vec<MatchRule>,
    pub walk_interval: Duration,
    pub receive_timeout: Duration,
    pub check_interval: Duration,
    pub eviction_interval: Duration,
    pub evict_older_than: Duration,
}
