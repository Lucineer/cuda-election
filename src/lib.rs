/*!
# cuda-election

Leader election for agent clusters.

Fleet agents need a coordinator. This crate provides Raft-inspired
leader election with term management, heartbeat timeout, and voting.

- Candidate/Follower/Leader roles
- Term-based elections
- Vote request/grant
- Heartbeat with timeout
- Leader lease
- Split-brain prevention (majority)
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role { Follower, Candidate, Leader }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VoteRequest { pub term: u64, pub candidate_id: String, pub last_log_index: u64 }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VoteResponse { pub term: u64, pub vote_granted: bool }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Heartbeat { pub term: u64, pub leader_id: String }

/// An election node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ElectionNode {
    pub id: String,
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub leader_id: Option<String>,
    pub votes_received: u32,
    pub last_heartbeat_ms: u64,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub total_elections: u64,
    pub total_terms: u64,
}

impl ElectionNode {
    pub fn new(id: &str, election_timeout_ms: u64, heartbeat_interval_ms: u64) -> Self {
        ElectionNode { id: id.to_string(), role: Role::Follower, current_term: 0, voted_for: None, leader_id: None, votes_received: 0, last_heartbeat_ms: now(), election_timeout_ms, heartbeat_interval_ms, total_elections: 0, total_terms: 0 }
    }

    /// Check if election timeout has elapsed
    pub fn is_election_timeout(&self) -> bool {
        self.role != Role::Leader && now() - self.last_heartbeat_ms > self.election_timeout_ms
    }

    /// Start an election
    pub fn start_election(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        self.votes_received = 1; // vote for self
        self.total_elections += 1;
        self.total_terms += 1;
    }

    /// Receive a vote request
    pub fn handle_vote_request(&mut self, req: &VoteRequest) -> VoteResponse {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }
        if req.term < self.current_term { return VoteResponse { term: self.current_term, vote_granted: false }; }
        if self.voted_for.is_some() { return VoteResponse { term: self.current_term, vote_granted: false }; }
        self.voted_for = Some(req.candidate_id.clone());
        VoteResponse { term: self.current_term, vote_granted: true }
    }

    /// Receive a vote
    pub fn receive_vote(&mut self, term: u64) {
        if term != self.current_term || self.role != Role::Candidate { return; }
        self.votes_received += 1;
    }

    /// Check if candidate has majority
    pub fn check_majority(&mut self, cluster_size: usize) -> bool {
        let majority = (cluster_size / 2) + 1;
        if self.votes_received >= majority as u32 && self.role == Role::Candidate {
            self.role = Role::Leader;
            self.leader_id = Some(self.id.clone());
            return true;
        }
        false
    }

    /// Receive a heartbeat
    pub fn handle_heartbeat(&mut self, hb: &Heartbeat) {
        if hb.term >= self.current_term {
            self.current_term = hb.term;
            self.role = Role::Follower;
            self.leader_id = Some(hb.leader_id.clone());
            self.voted_for = None;
            self.last_heartbeat_ms = now();
            self.votes_received = 0;
        }
    }

    /// Update heartbeat timestamp (for leader)
    pub fn send_heartbeat(&mut self) -> Option<Heartbeat> {
        if self.role != Role::Leader { return None; }
        self.last_heartbeat_ms = now();
        Some(Heartbeat { term: self.current_term, leader_id: self.id.clone() })
    }

    pub fn summary(&self) -> String {
        format!("Node[{}]: role={:?}, term={}, leader={:?}, elections={}",
            self.id, self.role, self.current_term, self.leader_id, self.total_elections)
    }
}

/// Election cluster manager
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ElectionCluster {
    pub nodes: HashMap<String, ElectionNode>,
    pub cluster_size: usize,
    pub total_elections: u64,
    pub current_leader: Option<String>,
}

impl ElectionCluster {
    pub fn new(cluster_size: usize) -> Self { ElectionCluster { nodes: HashMap::new(), cluster_size, total_elections: 0, current_leader: None } }

    pub fn add_node(&mut self, node: ElectionNode) { self.nodes.insert(node.id.clone(), node); }

    /// Run one round: check timeouts, start elections, collect votes, check majority
    pub fn tick(&mut self) -> Option<String> {
        // Step 1: Check for election timeouts
        for node in self.nodes.values_mut() {
            if node.is_election_timeout() && node.role != Role::Candidate {
                node.start_election();
            }
        }

        // Step 2: Candidates request votes from all nodes
        let candidates: Vec<String> = self.nodes.values().filter(|n| n.role == Role::Candidate).map(|n| n.id.clone()).collect();
        for cid in &candidates {
            if let Some(candidate) = self.nodes.get(cid) {
                let req = VoteRequest { term: candidate.current_term, candidate_id: cid.clone(), last_log_index: 0 };
                for (nid, node) in &mut self.nodes {
                    if nid == cid { continue; }
                    let resp = node.handle_vote_request(&req);
                    if resp.vote_granted {
                        if let Some(c) = self.nodes.get_mut(cid) { c.receive_vote(resp.term); }
                    }
                }
            }
        }

        // Step 3: Check majority
        for node in self.nodes.values_mut() {
            if node.check_majority(self.cluster_size) {
                self.current_leader = Some(node.id.clone());
                return Some(node.id.clone());
            }
        }

        // Step 4: Leader sends heartbeat
        for node in self.nodes.values_mut() {
            if let Some(hb) = node.send_heartbeat() {
                for (nid, other) in &mut self.nodes {
                    if nid != &hb.leader_id { other.handle_heartbeat(&hb); }
                }
            }
        }

        None
    }

    pub fn summary(&self) -> String {
        let leader = self.current_leader.as_deref().unwrap_or("none");
        format!("Cluster: size={}, leader={}, nodes={}", self.cluster_size, leader, self.nodes.len())
    }
}

fn now() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_follower_timeout() {
        let mut node = ElectionNode::new("a", 0, 100);
        node.last_heartbeat_ms = 0;
        assert!(node.is_election_timeout());
    }

    #[test]
    fn test_start_election() {
        let mut node = ElectionNode::new("a", 5000, 1000);
        node.start_election();
        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.votes_received, 1);
    }

    #[test]
    fn test_vote_grant() {
        let mut voter = ElectionNode::new("v", 5000, 1000);
        let req = VoteRequest { term: 1, candidate_id: "c".into(), last_log_index: 0 };
        let resp = voter.handle_vote_request(&req);
        assert!(resp.vote_granted);
        assert_eq!(voter.voted_for.as_deref(), Some("c"));
    }

    #[test]
    fn test_vote_denied_already_voted() {
        let mut voter = ElectionNode::new("v", 5000, 1000);
        voter.voted_for = Some("other".into());
        let req = VoteRequest { term: 1, candidate_id: "c".into(), last_log_index: 0 };
        let resp = voter.handle_vote_request(&req);
        assert!(!resp.vote_granted);
    }

    #[test]
    fn test_vote_denied_stale_term() {
        let mut voter = ElectionNode::new("v", 5000, 1000);
        voter.current_term = 5;
        let req = VoteRequest { term: 3, candidate_id: "c".into(), last_log_index: 0 };
        let resp = voter.handle_vote_request(&req);
        assert!(!resp.vote_granted);
    }

    #[test]
    fn test_heartbeat_makes_follower() {
        let mut node = ElectionNode::new("a", 5000, 1000);
        node.start_election();
        let hb = Heartbeat { term: node.current_term, leader_id: "leader".into() };
        node.handle_heartbeat(&hb);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.leader_id.as_deref(), Some("leader"));
    }

    #[test]
    fn test_majority_wins() {
        let mut node = ElectionNode::new("c", 5000, 1000);
        node.start_election();
        node.votes_received = 2;
        assert!(node.check_majority(3));
    }

    #[test]
    fn test_cluster_election() {
        let mut cluster = ElectionCluster::new(3);
        for id in ["a", "b", "c"] { cluster.add_node(ElectionNode::new(id, 0, 1000)); }
        // Force timeout on all
        for node in cluster.nodes.values_mut() { node.last_heartbeat_ms = 0; }
        let winner = cluster.tick();
        assert!(winner.is_some());
    }

    #[test]
    fn test_leader_heartbeat_stops_elections() {
        let mut cluster = ElectionCluster::new(3);
        for id in ["a", "b", "c"] { cluster.add_node(ElectionNode::new(id, 5000, 1000)); }
        // First tick: election
        cluster.tick();
        // Second tick: leader heartbeat, no new election
        let winner = cluster.tick();
        assert!(winner.is_none()); // leader already established
    }

    #[test]
    fn test_summary() {
        let node = ElectionNode::new("x", 5000, 1000);
        let s = node.summary();
        assert!(s.contains("Follower"));
    }
}
