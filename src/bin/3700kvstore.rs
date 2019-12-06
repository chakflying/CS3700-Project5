#![allow(non_snake_case)]

#[macro_use]
extern crate cute;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate rand;

use std::time::{Duration, Instant};

use clap::{App, Arg};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use unix_socket::UnixSeqpacket;

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Clone)]
struct Message {
    src: String,
    dst: String,
    leader: String,
    #[serde(rename = "type")]
    m_type: String,
    #[serde(default)]
    MID: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    ae_message: Option<AEMessage>,
    #[serde(default)]
    prevLog: Option<(usize, u32)>,
    #[serde(default)]
    term: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Clone)]
struct AEMessage {
    term: u32,
    committedID: usize,
    prevLog: Option<(usize, u32)>,
    entries: Vec<LogEntry>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
enum Operation {
    Put(String, String),
}

impl Default for Operation {
    fn default() -> Operation {
        Operation::Put("".into(), "".into())
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Clone)]
struct LogEntry {
    id: usize,
    term: u32,
    operation: Operation,
    src: String,
    MID: String,
}

#[derive(Default)]
struct State {
    masterState: u8,
    myID: String,
    currentTerm: u32,
    votedFor: Option<String>,
    leader: String,
    log: Vec<LogEntry>,
    committedID: usize,
    lastApplied: usize,
    swarmSize: usize,
    storage: HashMap<String, String>,
}

impl State {
    fn new() -> Self {
        Default::default()
    }
    fn apply_committed(&mut self) {
        if self.committedID > self.lastApplied {
            debug!("[{}] Entering Apply committed", self.myID);
            for i in self.lastApplied + 1..=self.committedID {
                debug!("[{}] committing entry {}", self.myID, i);
                match &self.log[i-1].operation {
                    Operation::Put(x, y) => {
                        self.storage.insert(x.clone(), y.clone());
                    }
                }
            }
            self.lastApplied = self.committedID;
        }
    }
    fn process_AE(&mut self, ae_message: &AEMessage) -> bool {
        debug!("[{}] Entered process_AE.", self.myID);
        if ae_message.term < self.currentTerm {
            debug!("[{}] Wrong Term.", self.myID);
            return false;
        }
        if ae_message.prevLog != None && self.log.len() > 0 {
            let prevLog = ae_message.prevLog.unwrap();
            if prevLog.0 != 0 && ( prevLog.0-1 >= self.log.len() || self.log[prevLog.0-1].term != prevLog.1 ) {
                debug!("[{}] Wrong prevLog.", self.myID);
                return false;
            }
        }
        if ae_message.entries.len() == 0 { return true; }
        if ae_message.entries[0].id - 1 < self.log.len() {
            self.log.drain(ae_message.entries[0].id-1..);
        } else if ae_message.entries[0].id - 1 > self.log.len() {
            panic!("Received AE message with missing logs!");
        }
        for entry in ae_message.entries.iter() {
            self.log.push(entry.clone());
        }
        self.committedID = ae_message.committedID;
        self.apply_committed();
        return true;
    }
    fn AE_ok(&self, stream: &UnixSeqpacket, msg: &Message, result: bool) {
        let message = Message {
            src: self.myID.clone(),
            dst: msg.src.clone(),
            leader: self.leader.clone(),
            m_type: "AE-ok".into(),
            MID: msg.MID.clone(),
            prevLog: if self.log.len() > 0 {
                Some((self.log.len(), self.log.last().unwrap().term))
            } else {
                Some((0, 0))
            },
            term: Some(self.currentTerm),
            value: if result {
                "true".into()
            } else {
                "false".into()
            },
            ..Default::default()
        };
        debug!("[{}] Replying AE-ok with result {} and prevLog {:?}", self.myID, result, message.prevLog);
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn request_vote(&self, stream: &UnixSeqpacket) {
        let mut rng = rand::thread_rng();
        let message = Message {
            src: self.myID.clone(),
            dst: "FFFF".into(),
            leader: "FFFF".into(),
            m_type: "request-vote".into(),
            MID: format!("{:X}", rng.gen::<u64>()),
            term: Some(self.currentTerm),
            prevLog: if self.log.len() > 0 {
                Some((self.log.len() - 1, self.log.last().unwrap().term))
            } else {
                Some((0, 0))
            },
            ..Default::default()
        };
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn vote(&self, stream: &UnixSeqpacket, dst: &str) {
        let mut rng = rand::thread_rng();
        let message = Message {
            src: self.myID.clone(),
            dst: dst.into(),
            leader: "FFFF".into(),
            m_type: "vote".into(),
            MID: format!("{:X}", rng.gen::<u64>()),
            ..Default::default()
        };
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn get_ok(&self, stream: &UnixSeqpacket, msg: &Message, value: &String) {
        let message = Message {
            src: self.myID.clone(),
            dst: msg.src.clone(),
            leader: self.leader.clone(),
            m_type: "ok".into(),
            MID: msg.MID.clone(),
            value: value.clone(),
            ..Default::default()
        };
        debug!("[{}] Replying get_ok. {:?}", self.myID, message);
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn put_ok(&self, stream: &UnixSeqpacket, src: &String, MID: &String) {
        let message = Message {
            src: self.myID.clone(),
            dst: src.clone(),
            leader: self.leader.clone(),
            m_type: "ok".into(),
            MID: MID.clone(),
            ..Default::default()
        };
        debug!("[{}] Replying put_ok. {:?}", self.myID, message);
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn redirect(&self, stream: &UnixSeqpacket, msg: &Message) {
        let message = Message {
            src: self.myID.clone(),
            dst: msg.src.clone(),
            leader: self.leader.clone(),
            m_type: "redirect".into(),
            MID: msg.MID.clone(),
            ..Default::default()
        };
//        debug!("[{}] Replying Redirect. {:?}", self.myID, message);
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn take_lead(&self, stream: &UnixSeqpacket) {
        debug!("[{}] Entering take_lead.", self.myID);
        let mut rng = rand::thread_rng();
        let message = Message {
            src: self.myID.clone(),
            dst: "FFFF".into(),
            leader: self.myID.clone(),
            m_type: "append-entries".into(),
            MID: format!("{:X}", rng.gen::<u64>()),
            ae_message: Some(AEMessage {
                term: self.currentTerm,
                committedID: self.committedID,
                prevLog: if self.log.len() > 0 {
                    Some((self.log.len() - 1, self.log.last().unwrap().term))
                } else {
                    Some((0, 0))
                },
                entries: vec![],
            }),
            ..Default::default()
        };
        debug!("[{}] Sending take_lead.", self.myID);
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn AE(&self, leader_state: &LeaderState, stream: &UnixSeqpacket) {
        debug!("[{}] Entering AE.", self.myID);
        let mut rng = rand::thread_rng();
        for (rep_name, rep_nextIndex) in leader_state.nextIndex.iter() {
            let message = Message {
                src: self.myID.clone(),
                dst: rep_name.into(),
                leader: self.myID.clone(),
                m_type: "append-entries".into(),
                MID: format!("{:X}", rng.gen::<u64>()),
                ae_message: Some(AEMessage {
                    term: self.currentTerm,
                    committedID: self.committedID,
                    prevLog: if self.log.len() > 0 && *rep_nextIndex > 1 {
                        Some((*rep_nextIndex - 1, self.log[*rep_nextIndex - 2].term))
                    } else {
                        Some((0, 0))
                    },
                    entries: self.log[*rep_nextIndex-1..].to_vec(),
                }),
                ..Default::default()
            };
//            debug!("[{}] Sending AE.", self.myID);
            stream
                .send(serde_json::to_string(&message).unwrap().as_bytes())
                .expect("Socket Send Error");
        }
    }
    fn retry_AE(&self, leader_state: &LeaderState, stream: &UnixSeqpacket, dst: &String) {
        debug!("[{}] Entering Retry AE. leader_state: {:#?} retrying on: {}.", self.myID, leader_state, dst);
        let mut rng = rand::thread_rng();
        let rep_nextIndex = leader_state.nextIndex.get(dst).unwrap();
        let message = Message {
            src: self.myID.clone(),
            dst: dst.into(),
            leader: self.myID.clone(),
            m_type: "append-entries".into(),
            MID: format!("{:X}", rng.gen::<u64>()),
            ae_message: Some(AEMessage {
                term: self.currentTerm,
                committedID: self.committedID,
                prevLog: if self.log.len() > 0 && *rep_nextIndex > 0 {
                    Some((*rep_nextIndex - 1, self.log[*rep_nextIndex - 1].term))
                } else {
                    Some((0, 0))
                },
                entries: self.log[*rep_nextIndex-1..].to_vec(),
            }),
            ..Default::default()
        };
//        debug!("[{}] Retry Sending AE.", self.myID);
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn handle_vote_request(&mut self, stream: &UnixSeqpacket, m: &Message) -> bool {
        debug!("[{}] received vote request.", self.myID);
        if self.votedFor == None || m.term.unwrap() > self.currentTerm {
            if m.prevLog != None && self.log.len() > 0 {
                if self.log.last().unwrap().term > m.prevLog.unwrap().1
                    || self.log.len() - 1 > m.prevLog.unwrap().0
                {
                    debug!("[{}] Candidate {} has older log, not voting.", self.myID, m.src);
                    return false;
                }
            }
            self.votedFor = Some(m.src.clone());
            self.currentTerm = m.term.unwrap();
            debug!("[{}] Giving vote to {} for term {}", self.myID, m.src, self.currentTerm);
            self.vote(stream, &m.src);
            return true;
        }
        debug!("[{}] Candidate {} has older term (me:{} he: {}) or I have voted, not voting.", self.myID, m.src, self.currentTerm, m.term.unwrap());
        return false;
    }
    fn fail(&self, stream: &UnixSeqpacket, msg: &Message) {
        let message = Message {
            src: self.myID.clone(),
            dst: msg.src.clone(),
            leader: self.leader.clone(),
            m_type: "fail".into(),
            MID: msg.MID.clone(),
            ..Default::default()
        };
        stream
            .send(serde_json::to_string(&message).unwrap().as_bytes())
            .expect("Socket Send Error");
    }
    fn process_AE_ok(&mut self, leader_state: &mut LeaderState, stream: &UnixSeqpacket, m: &Message) {
//        debug!("[{}] Processing AE-ok", self.myID);
        if m.value == "false" {
            if *leader_state.nextIndex.entry(m.src.clone()).or_default() > m.prevLog.unwrap().0 + 1 {
                *leader_state.nextIndex.entry(m.src.clone()).or_default() = m.prevLog.unwrap().0 + 1;
            } else {
                *leader_state.nextIndex.entry(m.src.clone()).or_default() -= 1;
            }
            self.retry_AE(&leader_state, &stream, &m.src);
        } else {
            if m.prevLog.unwrap().0 == 0 { return; }
            if m.prevLog.unwrap().0 >= *leader_state.nextIndex.get(&m.src).unwrap() {
                for i in *leader_state.matchIndex.get(&m.src).unwrap()+1..=m.prevLog.unwrap().0 {
                    leader_state.pendingOp.entry(i).or_default().insert(m.src.clone());
                    debug!("[{}] Entry {} now has {} replication.", self.myID, i, leader_state.pendingOp.get(&i).unwrap().len());
                    if leader_state.pendingOp.entry(i).or_default().len() + 1 > self.swarmSize / 2 && i > self.committedID && self.log[i-1].term == self.currentTerm
                    {
                        debug!("[{}] Entry {} replicated on majority, committing.", self.myID, i);
                        self.committedID = i;
                        debug!("[{}] committedID is now {}", self.myID, self.committedID);
                        let entry = &self.log[i-1];
                        match entry.operation {
                            Operation::Put(_,_) => self.put_ok(&stream, &entry.src, &entry.MID)
                        }
                    }
                    if leader_state.pendingOp.entry(i).or_default().len() + 1 == self.swarmSize
                    {
                        debug!("[{}] Entry {} all replicated, removing from pending.", self.myID, i);
                        leader_state.pendingOp.remove(&i);
                    }

                }
                leader_state.nextIndex.insert(m.src.clone(), m.prevLog.unwrap().0 + 1);
                leader_state.matchIndex.insert(m.src.clone(), m.prevLog.unwrap().0);
            }
        }
    }
}

#[derive(Debug)]
struct FollowerState {
    lastReceived: Instant,
    electionTimeout: Duration,
}

#[derive(Debug)]
struct CandidateState {
    electionStarted: Instant,
    electionTimeout: Duration,
    votes: HashMap<String, u8>,
    votes_received: usize,
}

#[derive(Debug)]
struct LeaderState {
    lastHeartbeat: Instant,
    heartbeatTimeout: Duration,
    nextIndex: HashMap<String, usize>,
    matchIndex: HashMap<String, usize>,
    pendingOp: HashMap<usize, HashSet<String>>,
    pendingMsg: HashMap<usize, Message>,
}

fn main() {
    pretty_env_logger::init();

    let args = App::new("CS3700 Project 5")
        .author("Nelson Chan <chan.chak@husky.neu.edu>")
        .arg(
            Arg::with_name("my ID")
                .index(1)
                .required(true)
                .help("ID of this replica"),
        )
        .arg(
            Arg::with_name("replica IDs")
                .index(2)
                .required(true)
                .multiple(true)
                .help("IDs of other replicas"),
        )
        .get_matches();

    let arg_my_ID = args
        .value_of("my ID")
        .expect("ID of this replica not provided.");
    let arg_replica_IDs: Vec<_> = args
        .values_of("replica IDs")
        .expect("replicas not provided")
        .collect();

    debug!("my ID: {}, Replicas: {:?}", arg_my_ID, arg_replica_IDs);

    let stream = UnixSeqpacket::connect(arg_my_ID).expect("Cannot connect to socket.");
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .expect("Setting read timeout failed");

    let mut state = State::new();
    state.swarmSize = arg_replica_IDs.len() + 1;
    state.myID = arg_my_ID.into();
    state.leader = "FFFF".into();
    let mut buf = [0; 16892];

    loop {
        match state.masterState {
            0 => {
                let mut follower_state = FollowerState {
                    lastReceived: Instant::now(),
                    electionTimeout: Duration::from_millis(rand::thread_rng().gen_range(150, 300)),
                };
                info!("[{}] Entering Follower State. Timeout: {}ms", state.myID, follower_state.electionTimeout.as_millis());
                loop {
                    let count = match stream.recv(&mut buf) {
                        Ok(c) => c,
                        Err(_err) => {
                            //                            debug!("[{}] Receive error: {}", state.myID, err);
                            0
                        }
                    };
                    if count != 0 {
                        let m: Message = serde_json::from_str(&String::from_utf8_lossy(&buf[..count])).expect("parse JSON failed");
                        if m.term != None && m.term.unwrap() > state.currentTerm {
                            state.currentTerm = m.term.unwrap();
                            state.votedFor = None;
                            debug!("[{}] higher term {} found, updating.", state.myID, m.term.unwrap());
                        }
                        match m.m_type.as_ref() {
                            "request-vote" => {
                                if state.handle_vote_request(&stream, &m) {
                                    follower_state.lastReceived = Instant::now();
                                }
                            }
                            "get" => {
                                state.redirect(&stream, &m);
                            }
                            "put" => {
                                state.redirect(&stream, &m);
                            }
                            "append-entries" => {
                                follower_state.lastReceived = Instant::now();
                                if m.ae_message != None {
                                    let ae_message = m.ae_message.clone().unwrap();
                                    if state.leader == "FFFF" {
                                        state.leader = m.src.clone();
                                        state.currentTerm = ae_message.term;
                                    }
                                    if ae_message.term > state.currentTerm {
                                        debug!("[{}] Updating my Term and leader.", state.myID);
                                        state.leader = m.src.clone();
                                        state.currentTerm = ae_message.term;
                                    }
                                    let result = state.process_AE(&ae_message);
                                    state.AE_ok(&stream, &m, result);
                                }
                            }
                            _ => {}
                        }
                    }
                    if follower_state.lastReceived.elapsed() > follower_state.electionTimeout {
                        state.currentTerm += 1;
                        state.masterState = 1;
                        state.votedFor = Some(state.myID.clone());
                        state.leader = "FFFF".into();
                        debug!("[{}] Transitioning to Candidate State, Term {}.", state.myID, state.currentTerm);
                        break;
                    }
                }
            }
            1 => 'candidate: loop {
                let mut candidate_state = CandidateState {
                    electionStarted: Instant::now(),
                    electionTimeout: Duration::from_millis(rand::thread_rng().gen_range(150, 300)),
                    votes: HashMap::new(),
                    votes_received: 0,
                };
                info!("[{}] Entering Candidate State for term {}.", state.myID, state.currentTerm);
                state.request_vote(&stream);
                loop {
                    let count = match stream.recv(&mut buf) {
                        Ok(c) => c,
                        Err(_err) => {
                            //                            debug!("[{}] Receive error: {}", state.myID, err);
                            0
                        }
                    };
                    if count != 0 {
                        let m: Message =
                            serde_json::from_str(&String::from_utf8_lossy(&buf[..count])).expect("parse JSON failed");
                        if m.term != None && m.term.unwrap() > state.currentTerm {
                            state.currentTerm = m.term.unwrap();
                            state.masterState = 0;
                            state.votedFor = None;
                            debug!("[{}] higher term {} found, reverting to follower.", state.myID, m.term.unwrap());
                            break 'candidate;
                        }
                        match m.m_type.as_ref() {
                            "vote" => {
                                if !candidate_state.votes.contains_key(&m.src) {
                                    candidate_state.votes.insert(m.src, 1);
                                    candidate_state.votes_received += 1;
                                }
                            }
                            "request-vote" => {
                                if state.handle_vote_request(&stream, &m) {
                                    candidate_state.electionStarted = Instant::now();
                                }
                            }
                            "append-entries" => {
                                if m.ae_message != None {
                                    let ae_message = m.ae_message.unwrap();
                                    if ae_message.term == state.currentTerm {
                                        state.masterState = 0;
                                        state.votedFor = None;
                                        debug!(
                                            "[{}] Leader found, changing to follower for term {}.",
                                            state.myID, state.currentTerm
                                        );
                                        state.leader = m.src.into();
                                        state.process_AE(&ae_message);
                                        break 'candidate;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    if candidate_state.votes_received > state.swarmSize / 2 {
                        state.masterState = 2;
                        debug!(
                            "[{}] Enough votes received, changing to leader for term {}.",
                            state.myID, state.currentTerm
                        );
                        break 'candidate;
                    }
                    if candidate_state.electionStarted.elapsed() > candidate_state.electionTimeout {
                        state.currentTerm += 1;
                        state.masterState = 1;
                        state.votedFor = Some(state.myID.clone());
                        debug!("[{}] Voting timed out for term {}.", state.myID, state.currentTerm);
                        break;
                    }
                }
            },
            2 => {
                info!("[{}] Entering Leader State.", state.myID);
                let mut leader_state = LeaderState {
                    lastHeartbeat: Instant::now(),
                    heartbeatTimeout: Duration::from_millis(100),
                    nextIndex: Default::default(),
                    matchIndex: Default::default(),
                    pendingOp: Default::default(),
                    pendingMsg: Default::default(),
                };
                for rep in arg_replica_IDs.iter() {
                    leader_state.nextIndex.insert(rep.parse().unwrap(), state.log.len()+1);
                    leader_state.matchIndex.insert(rep.parse().unwrap(), 0);
                }
                state.leader = state.myID.clone();
                state.take_lead(&stream);
                'leader: loop {
                    let count = match stream.recv(&mut buf) {
                        Ok(c) => c,
                        Err(_err) => {
                            //                            debug!("Receive error: {}", err);
                            0
                        }
                    };
                    if count != 0 {
                        let m: Message =
                            serde_json::from_str(&String::from_utf8_lossy(&buf[..count])).expect("parse JSON failed");
                        if m.term != None && state.currentTerm < m.term.unwrap() {
                            state.currentTerm = m.term.unwrap();
                            state.votedFor = None;
                            state.masterState = 0;
                            state.leader = "FFFF".into();
                            debug!("[{}] Found new term {}, reverting to follower.", state.myID, m.term.unwrap());
                            break 'leader;
                        }
                        match m.m_type.as_ref() {
                            "request-vote" => {
                                if state.handle_vote_request(&stream, &m) {
                                    state.masterState = 0;
                                    state.leader = "FFFF".into();
                                    debug!("[{}] Found new term {}, reverting to follower.", state.myID, m.term.unwrap());
                                    break 'leader;
                                }
                            }
                            "get" => {
                                debug!("[{}] Received get {}", state.myID, m.key);
                                state.get_ok(
                                    &stream,
                                    &m,
                                    state.storage.get(&m.key).unwrap_or(&String::from("")),
                                );
                            }
                            "put" => {
                                let new_entry = LogEntry {
                                    id: state.log.len()+1,
                                    term: state.currentTerm,
                                    operation: Operation::Put(m.key.clone(), m.value.clone()),
                                    src: m.src.clone(),
                                    MID: m.MID.clone(),
                                };
                                debug!("[{}] Creating new entry : {:?}", state.myID, new_entry);
                                leader_state.pendingOp.insert(new_entry.id, HashSet::new());
                                leader_state.pendingMsg.insert(new_entry.id, m.clone());
                                state.log.push(new_entry);
                            }
                            "AE-ok" => {
                                if m.term.unwrap() > state.currentTerm {
                                    state.masterState = 0;
                                    break 'leader;
                                }
                                state.process_AE_ok(&mut leader_state, &stream, &m);
                                state.apply_committed();
                            }
                            _ => {}
                        }
                    }
                    if leader_state.lastHeartbeat.elapsed() > leader_state.heartbeatTimeout {
                        debug!("[{}] Heartbeat timed out, sending AE.", state.myID);
                        state.AE(&leader_state, &stream);
                        leader_state.lastHeartbeat = Instant::now();
                    }
                }
            }
            _ => {}
        }
    }
}
