use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

pub type Address = String;
pub type BundleHash = String;
pub type Cloid = String;
pub type Decimal = String;
pub type Hash = String;
pub type Hex = String;

pub type AssetId = u32;
pub type Millis = u64;
pub type Nonce = u64;
pub type OrderId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeFillsData {
    pub local_time: String,
    pub block_time: String,
    pub block_number: u64,
    pub events: Vec<NodeFillEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeFillEvent(pub Address, pub NodeFill);

impl NodeFillEvent {
    pub fn user(&self) -> &str {
        &self.0
    }

    pub fn fill(&self) -> &NodeFill {
        &self.1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeFill {
    pub coin: String,
    pub px: Decimal,
    pub sz: Decimal,
    pub side: String,
    pub time: Millis,
    #[serde(rename = "startPosition")]
    pub start_position: Decimal,
    pub dir: String,
    #[serde(rename = "closedPnl")]
    pub closed_pnl: Decimal,
    pub hash: Hash,
    pub oid: OrderId,
    pub crossed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liquidation: Option<NodeFillLiquidation>,
    pub fee: Decimal,
    #[serde(
        default,
        rename = "builderFee",
        skip_serializing_if = "Option::is_none"
    )]
    pub builder_fee: Option<Decimal>,
    pub tid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloid: Option<Cloid>,
    #[serde(rename = "feeToken")]
    pub fee_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub builder: Option<Address>,
    #[serde(default, rename = "twapId", skip_serializing_if = "Option::is_none")]
    pub twap_id: Option<u64>,
    #[serde(
        default,
        rename = "deployerFee",
        skip_serializing_if = "Option::is_none"
    )]
    pub deployer_fee: Option<Decimal>,
    #[serde(
        default,
        rename = "priorityGas",
        skip_serializing_if = "Option::is_none"
    )]
    pub priority_gas: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeFillLiquidation {
    #[serde(rename = "liquidatedUser")]
    pub liquidated_user: Address,
    #[serde(rename = "markPx")]
    pub mark_px: Decimal,
    pub method: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hip3OracleUpdatesData {
    pub local_time: String,
    pub block_time: String,
    pub block_number: u64,
    pub events: Vec<Hip3OracleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hip3OracleUpdate {
    pub update_class: String,
    pub mark_px_inputs: Vec<Hip3OraclePxInput>,
    pub spot_px_inputs: Vec<Hip3OraclePxInput>,
    pub external_perp_px_inputs: Vec<Hip3OraclePxInput>,
    pub oracle_pxs: Hip3OraclePxs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hip3OraclePxInput(pub String, pub Decimal);

impl Hip3OraclePxInput {
    pub fn coin(&self) -> &str {
        &self.0
    }

    pub fn px(&self) -> &str {
        &self.1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hip3OraclePxs {
    pub coin_to_mark_px: Vec<Hip3OraclePxEntry>,
    pub coin_to_oracle_px: Vec<Hip3OraclePxEntry>,
    pub coin_to_external_perp_px: Vec<Hip3OraclePxEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hip3OraclePxEntry(pub String, pub Hip3OraclePx);

impl Hip3OraclePxEntry {
    pub fn coin(&self) -> &str {
        &self.0
    }

    pub fn px(&self) -> &Hip3OraclePx {
        &self.1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hip3OraclePx {
    pub px: Decimal,
    pub last_update_time: String,
    pub daily_px: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MiscEventsData {
    pub local_time: String,
    pub block_time: String,
    pub block_number: u64,
    pub events: Vec<MiscEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MiscEvent {
    pub time: String,
    pub hash: Hash,
    pub inner: Map<String, Value>,
}

impl MiscEvent {
    /// Returns the event kind and payload from the externally-tagged `inner` object.
    pub fn kind_and_payload(&self) -> Result<(&str, &Value), &'static str> {
        if self.inner.len() != 1 {
            return Err("misc event inner must contain exactly one event kind");
        }

        self.inner
            .iter()
            .next()
            .map(|(kind, payload)| (kind.as_str(), payload))
            .ok_or("misc event inner must contain exactly one event kind")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeTwapStatusesData {
    pub local_time: String,
    pub block_time: String,
    pub block_number: u64,
    pub events: Vec<NodeTwapStatusEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeTwapStatusEvent {
    pub time: String,
    pub twap_id: u64,
    pub state: NodeTwapState,
    pub status: TwapStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeTwapState {
    pub coin: String,
    pub user: Address,
    pub side: String,
    pub sz: Decimal,
    pub executed_sz: Decimal,
    pub executed_ntl: Decimal,
    pub minutes: u64,
    pub reduce_only: bool,
    pub randomize: bool,
    pub timestamp: Millis,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TwapStatus {
    Named(String),
    Error { error: String },
}

impl TwapStatus {
    pub fn into_parts(self) -> (String, Option<String>) {
        match self {
            Self::Named(status) => (status, None),
            Self::Error { error } => ("error".to_string(), Some(error)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemAndCoreWriterActionsData {
    pub local_time: String,
    pub block_time: String,
    pub block_number: u64,
    pub events: Vec<SystemAndCoreWriterActionEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemAndCoreWriterActionEvent {
    pub user: Address,
    pub nonce: Nonce,
    pub evm_tx_hash: Hash,
    pub action: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockData {
    #[serde(rename = "abci_block")]
    pub abci_block: AbciBlockIn,
    pub resps: Option<BlockResponses>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AbciBlockIn {
    pub time: String,
    pub round: u64,
    pub parent_round: u64,
    pub proposer: Address,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardfork: Option<Hardfork>,
    pub signed_action_bundles: Vec<SignedActionBundleEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hardfork {
    pub version: u64,
    pub round: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedActionBundleEntry(pub BundleHash, pub SignedActionBundle);

impl SignedActionBundleEntry {
    pub fn hash(&self) -> &str {
        &self.0
    }

    pub fn bundle(&self) -> &SignedActionBundle {
        &self.1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedActionBundle {
    pub signed_actions: Vec<SignedAction>,
    pub broadcaster: Address,
    pub broadcaster_nonce: Nonce,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedAction {
    pub signature: Signature,
    #[serde(
        default,
        rename = "vaultAddress",
        skip_serializing_if = "Option::is_none"
    )]
    pub vault_address: Option<Address>,
    pub action: Action,
    pub nonce: Nonce,
    #[serde(
        default,
        rename = "expiresAfter",
        skip_serializing_if = "Option::is_none"
    )]
    pub expires_after: Option<Millis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature {
    pub r: Hex,
    pub s: Hex,
    pub v: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockResponses {
    #[serde(rename = "Full")]
    pub full: Option<Vec<ResponseBundleEntry>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseBundleEntry(pub BundleHash, pub Vec<ActionResponse>);

impl ResponseBundleEntry {
    pub fn hash(&self) -> &str {
        &self.0
    }

    pub fn responses(&self) -> &[ActionResponse] {
        &self.1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionResponse {
    #[serde(default, deserialize_with = "null_to_default_string")]
    pub user: Address,
    pub res: ActionResult,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionResult {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<ExecutionResponse>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExecutionResponse {
    Typed(TypedExecutionResponse),
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypedExecutionResponse {
    #[serde(rename = "type")]
    pub response_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Action {
    #[serde(rename = "order")]
    Order(OrderAction),
    #[serde(rename = "cancel")]
    Cancel(CancelAction),
    #[serde(rename = "cancelByCloid")]
    CancelByCloid(CancelByCloidAction),
    #[serde(rename = "batchModify")]
    BatchModify(BatchModifyAction),
    #[serde(rename = "modify")]
    Modify(ModifyAction),
    #[serde(rename = "scheduleCancel")]
    ScheduleCancel(ScheduleCancelAction),
    #[serde(rename = "twapOrder")]
    TwapOrder(TwapOrderAction),
    #[serde(rename = "twapCancel")]
    TwapCancel(TwapCancelAction),
    #[serde(rename = "spotSend")]
    SpotSend(SpotSendAction),
    #[serde(rename = "sendAsset")]
    SendAsset(SendAssetAction),
    #[serde(rename = "agentSendAsset")]
    AgentSendAsset(GenericActionFields),
    #[serde(rename = "usdClassTransfer")]
    UsdClassTransfer(UsdClassTransferAction),
    #[serde(rename = "withdraw3")]
    Withdraw3(Withdraw3Action),
    #[serde(rename = "usdSend")]
    UsdSend(UsdSendAction),
    #[serde(rename = "updateLeverage")]
    UpdateLeverage(UpdateLeverageAction),
    #[serde(rename = "updateIsolatedMargin")]
    UpdateIsolatedMargin(UpdateIsolatedMarginAction),
    #[serde(rename = "userPortfolioMargin")]
    UserPortfolioMargin(GenericActionFields),
    #[serde(rename = "approveAgent")]
    ApproveAgent(ApproveAgentAction),
    #[serde(rename = "approveBuilderFee")]
    ApproveBuilderFee(ApproveBuilderFeeAction),
    #[serde(rename = "userDexAbstraction")]
    UserDexAbstraction(UserDexAbstractionAction),
    #[serde(rename = "agentEnableDexAbstraction")]
    AgentEnableDexAbstraction(GenericActionFields),
    #[serde(rename = "userSetAbstraction")]
    UserSetAbstraction(UserSetAbstractionAction),
    #[serde(rename = "agentSetAbstraction")]
    AgentSetAbstraction(AgentSetAbstractionAction),
    #[serde(rename = "subAccountTransfer")]
    SubAccountTransfer(SubAccountTransferAction),
    #[serde(rename = "createSubAccount")]
    CreateSubAccount(CreateSubAccountAction),
    #[serde(rename = "subAccountModify")]
    SubAccountModify(GenericActionFields),
    #[serde(rename = "subAccountSpotTransfer")]
    SubAccountSpotTransfer(GenericActionFields),
    #[serde(rename = "createVault")]
    CreateVault(GenericActionFields),
    #[serde(rename = "vaultTransfer")]
    VaultTransfer(VaultTransferAction),
    #[serde(rename = "NetChildVaultPositionsAction")]
    NetChildVaultPositions(GenericActionFields),
    #[serde(rename = "VoteEthDepositAction")]
    VoteEthDeposit(GenericActionFields),
    #[serde(rename = "VoteEthFinalizedWithdrawalAction")]
    VoteEthFinalizedWithdrawal(GenericActionFields),
    #[serde(rename = "ValidatorSignWithdrawalAction")]
    ValidatorSignWithdrawal(GenericActionFields),
    #[serde(rename = "voteAppHash")]
    VoteAppHash(GenericActionFields),
    #[serde(rename = "validatorVote")]
    ValidatorVote(GenericActionFields),
    #[serde(rename = "evmRawTx")]
    EvmRawTx(EvmRawTxAction),
    #[serde(rename = "evmUserModify")]
    EvmUserModify(GenericActionFields),
    #[serde(rename = "multiSig")]
    MultiSig(GenericActionFields),
    #[serde(rename = "convertToMultiSigUser")]
    ConvertToMultiSigUser(GenericActionFields),
    #[serde(rename = "setReferrer")]
    SetReferrer(SetReferrerAction),
    #[serde(rename = "registerReferrer")]
    RegisterReferrer(RegisterReferrerAction),
    #[serde(rename = "perpDeploy")]
    PerpDeploy(GenericActionFields),
    #[serde(rename = "claimRewards")]
    ClaimRewards(GenericActionFields),
    #[serde(rename = "tokenDelegate")]
    TokenDelegate(TokenDelegateAction),
    #[serde(rename = "borrowLend")]
    BorrowLend(BorrowLendAction),
    #[serde(rename = "cDeposit")]
    CDeposit(CollateralTransferAction),
    #[serde(rename = "cWithdraw")]
    CWithdraw(CollateralTransferAction),
    #[serde(rename = "spotUser")]
    SpotUser(GenericActionFields),
    #[serde(rename = "noop")]
    Noop(GenericActionFields),
    #[serde(rename = "setGlobal")]
    SetGlobal(GenericActionFields),
    #[serde(rename = "SetGlobalAction")]
    SetGlobalAction(GenericActionFields),
    #[serde(rename = "reserveRequestWeight")]
    ReserveRequestWeight(ReserveRequestWeightAction),
    #[serde(rename = "gossipPriorityBid")]
    GossipPriorityBid(GenericActionFields),
    #[serde(other)]
    Unknown,
}

impl Action {
    pub fn action_type(&self) -> &str {
        match self {
            Self::Order(_) => "order",
            Self::Cancel(_) => "cancel",
            Self::CancelByCloid(_) => "cancelByCloid",
            Self::BatchModify(_) => "batchModify",
            Self::Modify(_) => "modify",
            Self::ScheduleCancel(_) => "scheduleCancel",
            Self::TwapOrder(_) => "twapOrder",
            Self::TwapCancel(_) => "twapCancel",
            Self::SpotSend(_) => "spotSend",
            Self::SendAsset(_) => "sendAsset",
            Self::AgentSendAsset(_) => "agentSendAsset",
            Self::UsdClassTransfer(_) => "usdClassTransfer",
            Self::Withdraw3(_) => "withdraw3",
            Self::UsdSend(_) => "usdSend",
            Self::UpdateLeverage(_) => "updateLeverage",
            Self::UpdateIsolatedMargin(_) => "updateIsolatedMargin",
            Self::UserPortfolioMargin(_) => "userPortfolioMargin",
            Self::ApproveAgent(_) => "approveAgent",
            Self::ApproveBuilderFee(_) => "approveBuilderFee",
            Self::UserDexAbstraction(_) => "userDexAbstraction",
            Self::AgentEnableDexAbstraction(_) => "agentEnableDexAbstraction",
            Self::UserSetAbstraction(_) => "userSetAbstraction",
            Self::AgentSetAbstraction(_) => "agentSetAbstraction",
            Self::SubAccountTransfer(_) => "subAccountTransfer",
            Self::CreateSubAccount(_) => "createSubAccount",
            Self::SubAccountModify(_) => "subAccountModify",
            Self::SubAccountSpotTransfer(_) => "subAccountSpotTransfer",
            Self::CreateVault(_) => "createVault",
            Self::VaultTransfer(_) => "vaultTransfer",
            Self::NetChildVaultPositions(_) => "NetChildVaultPositionsAction",
            Self::VoteEthDeposit(_) => "VoteEthDepositAction",
            Self::VoteEthFinalizedWithdrawal(_) => "VoteEthFinalizedWithdrawalAction",
            Self::ValidatorSignWithdrawal(_) => "ValidatorSignWithdrawalAction",
            Self::VoteAppHash(_) => "voteAppHash",
            Self::ValidatorVote(_) => "validatorVote",
            Self::EvmRawTx(_) => "evmRawTx",
            Self::EvmUserModify(_) => "evmUserModify",
            Self::MultiSig(_) => "multiSig",
            Self::ConvertToMultiSigUser(_) => "convertToMultiSigUser",
            Self::SetReferrer(_) => "setReferrer",
            Self::RegisterReferrer(_) => "registerReferrer",
            Self::PerpDeploy(_) => "perpDeploy",
            Self::ClaimRewards(_) => "claimRewards",
            Self::TokenDelegate(_) => "tokenDelegate",
            Self::BorrowLend(_) => "borrowLend",
            Self::CDeposit(_) => "cDeposit",
            Self::CWithdraw(_) => "cWithdraw",
            Self::SpotUser(_) => "spotUser",
            Self::Noop(_) => "noop",
            Self::SetGlobal(_) => "setGlobal",
            Self::SetGlobalAction(_) => "SetGlobalAction",
            Self::ReserveRequestWeight(_) => "reserveRequestWeight",
            Self::GossipPriorityBid(_) => "gossipPriorityBid",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GenericActionFields {
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainAuthFields {
    #[serde(
        default,
        rename = "signatureChainId",
        skip_serializing_if = "Option::is_none"
    )]
    pub signature_chain_id: Option<String>,
    #[serde(
        default,
        rename = "hyperliquidChain",
        skip_serializing_if = "Option::is_none"
    )]
    pub hyperliquid_chain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<Nonce>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderAction {
    pub orders: Vec<Order>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grouping: Option<OrderGrouping>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub builder: Option<BuilderFee>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub a: AssetId,
    pub b: bool,
    pub p: String,
    pub s: String,
    pub r: bool,
    pub t: OrderType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OrderGrouping {
    Named(String),
    Structured(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OrderType {
    Limit { limit: LimitOrderType },
    Trigger { trigger: TriggerOrderType },
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitOrderType {
    pub tif: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerOrderType {
    #[serde(rename = "isMarket")]
    pub is_market: bool,
    #[serde(rename = "triggerPx")]
    pub trigger_px: String,
    pub tpsl: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuilderFee {
    pub b: String,
    pub f: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAction {
    pub cancels: Vec<CancelRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub a: AssetId,
    pub o: OrderId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelByCloidAction {
    pub cancels: Vec<CancelByCloidRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelByCloidRequest {
    #[serde(alias = "a")]
    pub asset: AssetId,
    pub cloid: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModifyAction {
    pub oid: OrderLocator,
    pub order: Order,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchModifyAction {
    pub modifies: Vec<ModifyAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OrderLocator {
    Oid(OrderId),
    Cloid(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleCancelAction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<Millis>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TwapOrderAction {
    pub twap: TwapOrder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TwapOrder {
    pub a: AssetId,
    pub b: bool,
    pub s: String,
    pub r: bool,
    pub m: u64,
    pub t: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwapCancelAction {
    pub a: AssetId,
    pub t: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvmRawTxAction {
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateLeverageAction {
    pub asset: AssetId,
    #[serde(rename = "isCross")]
    pub is_cross: bool,
    pub leverage: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateIsolatedMarginAction {
    pub asset: AssetId,
    #[serde(rename = "isBuy")]
    pub is_buy: bool,
    pub ntli: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotSendAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub destination: String,
    pub token: String,
    pub amount: String,
    pub time: Millis,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendAssetAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub destination: String,
    #[serde(default, rename = "sourceDex", skip_serializing_if = "Option::is_none")]
    pub source_dex: Option<Value>,
    #[serde(
        default,
        rename = "destinationDex",
        skip_serializing_if = "Option::is_none"
    )]
    pub destination_dex: Option<Value>,
    pub token: String,
    pub amount: String,
    #[serde(
        default,
        rename = "fromSubAccount",
        skip_serializing_if = "Option::is_none"
    )]
    pub from_sub_account: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsdClassTransferAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub amount: String,
    #[serde(rename = "toPerp")]
    pub to_perp: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Withdraw3Action {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub destination: String,
    pub amount: String,
    pub time: Millis,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsdSendAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub destination: String,
    pub amount: String,
    pub time: Millis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApproveAgentAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    #[serde(rename = "agentAddress")]
    pub agent_address: String,
    #[serde(default, rename = "agentName", skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApproveBuilderFeeAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    #[serde(rename = "maxFeeRate")]
    pub max_fee_rate: String,
    pub builder: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserDexAbstractionAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub user: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserSetAbstractionAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub user: String,
    pub abstraction: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSetAbstractionAction {
    pub abstraction: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubAccountTransferAction {
    #[serde(rename = "subAccountUser")]
    pub sub_account_user: String,
    #[serde(rename = "isDeposit")]
    pub is_deposit: bool,
    pub usd: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSubAccountAction {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultTransferAction {
    #[serde(rename = "vaultAddress")]
    pub vault_address: String,
    #[serde(rename = "isDeposit")]
    pub is_deposit: bool,
    pub usd: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetReferrerAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterReferrerAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenDelegateAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub validator: String,
    pub wei: u64,
    #[serde(rename = "isUndelegate")]
    pub is_undelegate: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BorrowLendAction {
    pub operation: String,
    pub token: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollateralTransferAction {
    #[serde(flatten)]
    pub chain: ChainAuthFields,
    pub wei: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReserveRequestWeightAction {
    pub weight: u64,
}

pub fn action_type_from_value(value: &Value) -> Option<&str> {
    value.get("type").and_then(Value::as_str)
}

pub fn object_without_type(value: Value) -> Option<Map<String, Value>> {
    let mut object = value.as_object()?.clone();
    object.remove("type");
    Some(object)
}

fn null_to_default_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_hip3_oracle_update_stream_event() {
        let data: Hip3OracleUpdatesData = serde_json::from_str(include_str!(
            "../../../hl-data/hip3_oracle_updates_streaming"
        ))
        .expect("hip3 oracle update should parse");

        assert_eq!(data.block_number, 1061545103);
        assert_eq!(data.events.len(), 1);

        let event = &data.events[0];
        assert_eq!(event.update_class, "Deployer");
        assert_eq!(event.mark_px_inputs[0].coin(), "para:AVGO");
        assert_eq!(event.mark_px_inputs[0].px(), "368.41");
        assert_eq!(event.oracle_pxs.coin_to_mark_px[0].coin(), "para:AVGO");
        assert_eq!(event.oracle_pxs.coin_to_mark_px[0].px().px, "368.41");
        assert_eq!(
            event.oracle_pxs.coin_to_external_perp_px[2]
                .px()
                .last_update_time,
            "1970-01-01T00:00:00"
        );
    }

    #[test]
    fn parses_block_with_order_and_response() {
        let json_str = serde_json::to_string(&json!({
            "abci_block": {
                "time": "2026-05-06T17:01:42.626698090",
                "round": 986370000,
                "parent_round": 986369999,
                "proposer": "0x5ac99df645f3414876c816caa18b2d234024b487",
                "hardfork": { "version": 57, "round": 990500929 },
                "signed_action_bundles": [[
                    "0x186a72721d56123bc078df0cd57562381896682ac791f2bbc017c9ff410c007d",
                    {
                        "signed_actions": [{
                            "signature": {
                                "r": "0x60456811cbb587b34d6d4079e4f61213eccd38a35a6aeeccf26a63bc46f126bf",
                                "s": "0x1974eb26496d0399c4103b11f46c6ce8dcc2c077bf215a53d55e5a36a9075405",
                                "v": 28
                            },
                            "action": {
                                "type": "order",
                                "orders": [{
                                    "a": 226,
                                    "b": true,
                                    "p": "54.45",
                                    "s": "8.39",
                                    "r": false,
                                    "t": { "limit": { "tif": "Ioc" } }
                                }],
                                "grouping": "na"
                            },
                            "nonce": 1778086740743u64
                        }],
                        "broadcaster": "0x67e451964e0421f6e7d07be784f35c530667c2b3",
                        "broadcaster_nonce": 1778086740744u64
                    }
                ]]
            },
            "resps": {
                "Full": [[
                    "0x186a72721d56123bc078df0cd57562381896682ac791f2bbc017c9ff410c007d",
                    [{
                        "user": "0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00",
                        "res": {
                            "status": "ok",
                            "response": {
                                "type": "order",
                                "data": {
                                    "statuses": [{
                                        "resting": { "oid": 413584929606u64 }
                                    }]
                                }
                            }
                        }
                    }]
                ]]
            }
        }))
        .unwrap();

        let block: BlockData = serde_json::from_str(&json_str).expect("block should parse");

        assert_eq!(block.abci_block.round, 986370000);
        let action = &block.abci_block.signed_action_bundles[0]
            .bundle()
            .signed_actions[0]
            .action;
        assert_eq!(action.action_type(), "order");
        let Action::Order(order) = action else {
            panic!("expected order action");
        };
        assert_eq!(order.orders[0].a, 226);
    }

    #[test]
    fn parses_structured_grouping_and_hex_modify_oid() {
        let action: Action = serde_json::from_value(json!({
            "type": "batchModify",
            "modifies": [{
                "oid": "0x100000000000000018acfcc5ba1b99b3",
                "order": {
                    "a": 214,
                    "b": true,
                    "p": "567.75",
                    "s": "40.14",
                    "r": false,
                    "t": { "limit": { "tif": "Ioc" } },
                    "c": "0x000000000000000018acfcc5ba1b99b3"
                }
            }]
        }))
        .expect("action should parse");

        let Action::BatchModify(batch) = action else {
            panic!("expected batch modify");
        };
        assert_eq!(batch.modifies.len(), 1);
        assert!(matches!(batch.modifies[0].oid, OrderLocator::Cloid(_)));
    }

    #[test]
    fn parses_unknown_action_type() {
        let action: Action = serde_json::from_value(json!({
            "type": "futureAction",
            "payload": { "n": 1 }
        }))
        .expect("unknown action should parse");

        assert!(matches!(action, Action::Unknown));
        assert_eq!(action.action_type(), "unknown");
    }

    #[test]
    fn parses_null_response_user_as_empty_string() {
        let json_str = serde_json::to_string(&json!({
            "user": null,
            "res": {
                "status": "err",
                "response": "missing user"
            }
        }))
        .unwrap();

        let response: ActionResponse =
            serde_json::from_str(&json_str).expect("response should parse");

        assert!(response.user.is_empty());
    }

    #[test]
    fn serializes_known_action_with_type_field() {
        let action = Action::Noop(GenericActionFields::default());
        let value = serde_json::to_value(action).expect("action should serialize");
        assert_eq!(value["type"], "noop");
    }
}
