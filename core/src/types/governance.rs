//! Files defyining the types used in governance.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Display};

use borsh::{BorshDeserialize, BorshSerialize};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::key::RefTo;
use crate::proto::SignatureIndex;
use crate::types::address::Address;
use crate::types::hash::Hash;
use crate::types::key::{common, SigScheme};
use crate::types::storage::Epoch;
use crate::types::token::{Amount, SCALE};

/// Type alias for vote power
pub type VotePower = u128;

/// A PGF cocuncil composed of the address and spending cap
pub type Council = (Address, Amount);

/// The type of a governance vote with the optional associated Memo
#[derive(
    Debug,
    Clone,
    Hash,
    PartialEq,
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
    Eq,
)]
pub enum VoteType {
    /// A default vote without Memo
    Default,
    /// A vote for the PGF council
    PGFCouncil(BTreeSet<Council>),
    /// A vote for ETH bridge carrying the signature over the proposed message
    ETHBridge(Signature),
}

#[derive(
    Debug,
    Clone,
    Hash,
    PartialEq,
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
    Eq,
)]
/// The vote for a proposal
pub enum ProposalVote {
    /// Yes
    Yay(VoteType),
    /// No
    Nay,
}

impl ProposalVote {
    /// Check if a vote is yay
    pub fn is_yay(&self) -> bool {
        matches!(self, ProposalVote::Yay(_))
    }

    /// Check if vote is of type default
    pub fn is_default_vote(&self) -> bool {
        matches!(
            self,
            ProposalVote::Yay(VoteType::Default) | ProposalVote::Nay
        )
    }
}

impl Display for ProposalVote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposalVote::Yay(vote_type) => match vote_type {
                VoteType::Default => write!(f, "yay"),
                VoteType::PGFCouncil(councils) => {
                    writeln!(f, "yay with councils:")?;
                    for (address, spending_cap) in councils {
                        writeln!(
                            f,
                            "Council: {}, spending cap: {}",
                            address, spending_cap
                        )?
                    }

                    Ok(())
                }
                VoteType::ETHBridge(sig) => {
                    write!(f, "yay with signature: {:#?}", sig)
                }
            },

            ProposalVote::Nay => write!(f, "nay"),
        }
    }
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum ProposalVoteParseError {
    #[error("Invalid vote. Vote shall be yay or nay.")]
    InvalidVote,
}

/// The type of the tally
pub enum Tally {
    /// Default proposal
    Default,
    /// PGF proposal
    PGFCouncil(Council),
    /// ETH Bridge proposal
    ETHBridge,
}

/// The result of a proposal
pub enum TallyResult {
    /// Proposal was accepted with the associated value
    Passed(Tally),
    /// Proposal was rejected
    Rejected,
    /// A critical error in tally computation with an error message
    Failed(String),
}

/// The result with votes of a proposal
pub struct ProposalResult {
    /// The result of a proposal
    pub result: TallyResult,
    /// The total voting power during the proposal tally
    pub total_voting_power: VotePower,
    /// The total voting power from yay votes
    pub total_yay_power: VotePower,
    /// The total voting power from nay votes (unused at the moment)
    pub total_nay_power: VotePower,
}

impl Display for ProposalResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let percentage = Decimal::checked_div(
            self.total_yay_power.into(),
            self.total_voting_power.into(),
        )
        .unwrap_or_default();

        write!(
            f,
            "{} with {} yay votes over {} ({:.2}%)",
            self.result,
            self.total_yay_power / SCALE as u128,
            self.total_voting_power / SCALE as u128,
            percentage.checked_mul(100.into()).unwrap_or_default()
        )
    }
}

impl Display for TallyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TallyResult::Passed(vote) => match vote {
                Tally::Default | Tally::ETHBridge => write!(f, "passed"),
                Tally::PGFCouncil((council, cap)) => write!(
                    f,
                    "passed with PGF council address: {}, spending cap: {}",
                    council, cap
                ),
            },
            TallyResult::Rejected => write!(f, "rejected"),
            TallyResult::Failed(msg) => write!(f, "failed with: {}", msg),
        }
    }
}

/// The type of a governance proposal
#[derive(
    Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize,
)]
pub enum ProposalType {
    /// A default proposal with the optional path to wasm code
    Default(Option<String>),
    /// A PGF council proposal
    PGFCouncil,
    /// An ETH bridge proposal
    ETHBridge,
}

#[derive(
    Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize,
)]
/// The proposal structure
pub struct Proposal {
    /// The proposal id
    pub id: Option<u64>,
    /// The proposal content
    pub content: BTreeMap<String, String>,
    /// The proposal author address
    pub author: Address,
    /// The proposal type
    pub r#type: ProposalType,
    /// The epoch from which voting is allowed
    pub voting_start_epoch: Epoch,
    /// The epoch from which voting is stopped
    pub voting_end_epoch: Epoch,
    /// The epoch from which this changes are executed
    pub grace_epoch: Epoch,
}

impl Display for Proposal {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "id: {:?}, author: {:?}", self.id, self.author)
    }
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum ProposalError {
    #[error("Invalid proposal data.")]
    InvalidProposalData,
}

#[derive(
    Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize,
)]
/// The offline proposal structure
pub struct OfflineProposal {
    /// The proposal content
    pub content: BTreeMap<String, String>,
    /// The proposal author address
    pub author: Address,
    /// The epoch from which this changes are executed
    pub tally_epoch: Epoch,
    /// The signatures over proposal data
    pub signatures: BTreeSet<SignatureIndex>,
    /// The address corresponding to the signature pk
    pub address: Address,
}

impl OfflineProposal {
    /// Create an offline proposal with a signature
    pub fn new(
        proposal: Proposal,
        address: Address,
        signing_key: Vec<common::SecretKey>,
        pks_map: HashMap<common::PublicKey, u64>,
    ) -> Self {
        let content_serialized = serde_json::to_vec(&proposal.content)
            .expect("Conversion to bytes shouldn't fail.");
        let author_serialized = serde_json::to_vec(&proposal.author)
            .expect("Conversion to bytes shouldn't fail.");
        let tally_epoch_serialized = serde_json::to_vec(&proposal.grace_epoch)
            .expect("Conversion to bytes shouldn't fail.");
        let proposal_serialized = &[
            content_serialized,
            author_serialized,
            tally_epoch_serialized,
        ]
        .concat();
        let proposal_data_hash = Hash::sha256(proposal_serialized);

        let signatures_index = compute_signatures_index(
            &signing_key,
            &pks_map,
            &proposal_data_hash,
        );

        Self {
            content: proposal.content,
            author: proposal.author,
            tally_epoch: proposal.grace_epoch,
            signatures: signatures_index,
            address,
        }
    }

    /// Check whether the signature is valid or not
    pub fn check_signature(
        &self,
        pks_map: HashMap<common::PublicKey, u64>,
        threshold: u64,
    ) -> bool {
        let proposal_data_hash = self.compute_hash();
        if self.signatures.len() < threshold as usize {
            return false;
        }

        let pks_map_inverted: HashMap<u64, common::PublicKey> =
            pks_map.iter().map(|(k, v)| (*v, k.clone())).collect();

        let valid_signatures = compute_total_valid_signatures(
            &self.signatures,
            &pks_map_inverted,
            &proposal_data_hash,
        );

        valid_signatures >= threshold
    }

    /// Compute the hash of the proposal
    pub fn compute_hash(&self) -> Hash {
        let content_serialized = serde_json::to_vec(&self.content)
            .expect("Conversion to bytes shouldn't fail.");
        let author_serialized = serde_json::to_vec(&self.author)
            .expect("Conversion to bytes shouldn't fail.");
        let tally_epoch_serialized = serde_json::to_vec(&self.tally_epoch)
            .expect("Conversion to bytes shouldn't fail.");
        let proposal_serialized = &[
            content_serialized,
            author_serialized,
            tally_epoch_serialized,
        ]
        .concat();
        Hash::sha256(proposal_serialized)
    }
}

#[derive(
    Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize,
)]
/// The offline proposal structure
pub struct OfflineVote {
    /// The proposal data hash
    pub proposal_hash: Hash,
    /// The proposal vote
    pub vote: ProposalVote,
    /// The signature over proposal data
    pub signatures: BTreeSet<SignatureIndex>,
    /// The address corresponding to the signature pk
    pub address: Address,
}

impl OfflineVote {
    /// Create an offline vote for a proposal
    pub fn new(
        proposal: &OfflineProposal,
        vote: ProposalVote,
        address: Address,
        signing_key: Vec<common::SecretKey>,
        pks_map: HashMap<common::PublicKey, u64>,
    ) -> Self {
        let proposal_hash = proposal.compute_hash();
        let proposal_hash_data = proposal_hash
            .try_to_vec()
            .expect("Conversion to bytes shouldn't fail.");
        let proposal_vote_data = vote
            .try_to_vec()
            .expect("Conversion to bytes shouldn't fail.");

        let vote_hash =
            Hash::sha256([proposal_hash_data, proposal_vote_data].concat());

        let signatures_index =
            compute_signatures_index(&signing_key, &pks_map, &vote_hash);

        Self {
            proposal_hash,
            vote,
            signatures: signatures_index,
            address,
        }
    }

    /// compute the hash of a proposal
    pub fn compute_hash(&self) -> Hash {
        let proposal_hash_data = self
            .proposal_hash
            .try_to_vec()
            .expect("Conversion to bytes shouldn't fail.");
        let proposal_vote_data = self
            .vote
            .try_to_vec()
            .expect("Conversion to bytes shouldn't fail.");
        let vote_serialized =
            &[proposal_hash_data, proposal_vote_data].concat();

        Hash::sha256(vote_serialized)
    }

    /// Check whether the signature is valid or not
    pub fn check_signature(
        &self,
        pks_map: HashMap<common::PublicKey, u64>,
        threshold: u64,
    ) -> bool {
        let vote_data_hash = self.compute_hash();
        if self.signatures.len() < threshold as usize {
            return false;
        }

        let pks_map_inverted: HashMap<u64, common::PublicKey> =
            pks_map.iter().map(|(k, v)| (*v, k.clone())).collect();

        let valid_signatures = compute_total_valid_signatures(
            &self.signatures,
            &pks_map_inverted,
            &vote_data_hash,
        );

        valid_signatures >= threshold
    }
}

fn compute_total_valid_signatures(
    signatures: &BTreeSet<SignatureIndex>,
    index_to_pk_map: &HashMap<u64, common::PublicKey>,
    hashed_data: &Hash,
) -> u64 {
    signatures.iter().fold(0_u64, |acc, signature_index| {
        let public_key = index_to_pk_map.get(&signature_index.index);
        if let Some(pk) = public_key {
            let sig_check = common::SigScheme::verify_signature(
                pk,
                hashed_data,
                &signature_index.sig,
            );
            if sig_check.is_ok() {
                acc + 1
            } else {
                acc
            }
        } else {
            acc
        }
    })
}

fn compute_signatures_index(
    keys: &[common::SecretKey],
    pk_to_index_map: &HashMap<common::PublicKey, u64>,
    hashed_data: &Hash,
) -> BTreeSet<SignatureIndex> {
    keys.iter()
        .filter_map(|signing_key| {
            let pk = signing_key.ref_to();
            let pk_index = pk_to_index_map.get(&pk);
            if pk_index.is_some() {
                let signature =
                    common::SigScheme::sign(signing_key, hashed_data);
                Some(SignatureIndex::from_single_signature(signature))
            } else {
                None
            }
        })
        .collect::<BTreeSet<SignatureIndex>>()
}
