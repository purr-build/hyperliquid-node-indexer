use chrono::{TimeZone, Utc};
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use tailer::Event;
use types::{
    EvmAccessListItem, EvmBlockData, EvmBlocksAndReceiptsData, EvmPrecompileResult, EvmReceipt,
    EvmSignature, EvmTransaction,
};

use crate::{
    metrics::Metrics,
    storage::{
        Address, Hash, address_from_hex, hash, hash_from_hex, new_inserter, sig_from_hex,
        uint256_from_hex,
    },
    streams::{Stream, commit_metered, parse_datetime_nanos},
};

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct EvmBlockRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub local_time: chrono::DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub hash: Hash,
    pub parent_hash: Hash,
    pub sha3_uncles: Hash,
    pub miner: Address,
    pub state_root: Hash,
    pub transactions_root: Hash,
    pub receipts_root: Hash,
    pub logs_bloom: String,
    pub difficulty: Hash,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub extra_data: String,
    pub mix_hash: Hash,
    pub nonce: u64,
    pub base_fee_per_gas: Option<Hash>,
    pub withdrawals_root: Option<Hash>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_block_root: Option<Hash>,
    pub highest_precompile_address: Address,
    pub transaction_count: u32,
    pub system_transaction_count: u32,
    pub ommers: String,
    pub withdrawals: String,
}

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct EvmTransactionRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub transaction_index: u32,
    pub hash: Option<Hash>,
    pub is_system: bool,
    pub transaction_type: String,
    pub chain_id: Option<u64>,
    pub nonce: u64,
    pub gas: u64,
    pub gas_price: Option<Hash>,
    pub max_fee_per_gas: Option<Hash>,
    pub max_priority_fee_per_gas: Option<Hash>,
    pub from_address: Option<Address>,
    pub to_address: Option<Address>,
    pub value: Hash,
    pub input: String,
    pub access_list: String,
    pub signature_r: Option<Hash>,
    pub signature_s: Option<Hash>,
    pub signature_y_parity: Option<u8>,
}

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct EvmReceiptRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub transaction_index: u32,
    pub transaction_hash: Option<Hash>,
    pub is_system: bool,
    pub transaction_type: String,
    pub success: bool,
    pub cumulative_gas_used: u64,
    pub gas_used: u64,
    pub log_count: u32,
}

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct EvmLogRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub transaction_index: u32,
    pub transaction_hash: Option<Hash>,
    pub is_system: bool,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<Hash>,
    pub data: String,
}

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct EvmReadPrecompileCallRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub precompile_index: u32,
    pub call_index: u32,
    pub address: Address,
    pub input: String,
    pub gas_limit: u64,
    pub success: bool,
    pub gas_used: Option<u64>,
    pub output: String,
    pub error: String,
}

pub struct EvmBlocksAndReceiptsRows {
    pub block: EvmBlockRow,
    pub transactions: Vec<EvmTransactionRow>,
    pub receipts: Vec<EvmReceiptRow>,
    pub logs: Vec<EvmLogRow>,
    pub read_precompile_calls: Vec<EvmReadPrecompileCallRow>,
}

pub fn parse(mut line: Vec<u8>) -> anyhow::Result<EvmBlocksAndReceiptsRows> {
    let EvmBlocksAndReceiptsData(local_time, data) =
        simd_json::serde::from_slice::<EvmBlocksAndReceiptsData>(&mut line)?;
    parse_data(&local_time, data)
}

fn parse_data(local_time: &str, data: EvmBlockData) -> anyhow::Result<EvmBlocksAndReceiptsRows> {
    let local_time = parse_datetime_nanos(local_time)?;
    let contents = data.block.contents();
    let header = &contents.header.header;
    let block_number = parse_u64_hex(&header.number, "block number")?;
    let timestamp = parse_u64_hex(&header.timestamp, "block timestamp")?;
    let timestamp =
        i64::try_from(timestamp).map_err(|_| anyhow::anyhow!("block timestamp overflow"))?;
    let block_time = Utc
        .timestamp_opt(timestamp, 0)
        .single()
        .ok_or_else(|| anyhow::anyhow!("invalid block timestamp"))?;

    if contents.body.transactions.len() != data.receipts.len() {
        return Err(anyhow::anyhow!(
            "transaction/receipt count mismatch in EVM block {block_number}: {} transactions, {} receipts",
            contents.body.transactions.len(),
            data.receipts.len()
        ));
    }

    let regular_count = u32::try_from(contents.body.transactions.len())
        .map_err(|_| anyhow::anyhow!("EVM transaction count overflow"))?;
    let system_count = u32::try_from(data.system_txs.len())
        .map_err(|_| anyhow::anyhow!("EVM system transaction count overflow"))?;

    let block = EvmBlockRow {
        local_time,
        block_time,
        block_number,
        hash: parse_hash(&contents.header.hash, "block hash")?,
        parent_hash: parse_hash(&header.parent_hash, "parent hash")?,
        sha3_uncles: parse_hash(&header.sha3_uncles, "sha3 uncles")?,
        miner: parse_address(&header.miner, "miner")?,
        state_root: parse_hash(&header.state_root, "state root")?,
        transactions_root: parse_hash(&header.transactions_root, "transactions root")?,
        receipts_root: parse_hash(&header.receipts_root, "receipts root")?,
        logs_bloom: header.logs_bloom.clone(),
        difficulty: parse_uint256(&header.difficulty, "difficulty")?,
        gas_limit: parse_u64_hex(&header.gas_limit, "gas limit")?,
        gas_used: parse_u64_hex(&header.gas_used, "gas used")?,
        extra_data: header.extra_data.clone(),
        mix_hash: parse_hash(&header.mix_hash, "mix hash")?,
        nonce: parse_u64_hex(&header.nonce, "nonce")?,
        base_fee_per_gas: header
            .base_fee_per_gas
            .as_deref()
            .map(|value| parse_uint256(value, "base fee per gas"))
            .transpose()?,
        withdrawals_root: header
            .withdrawals_root
            .as_deref()
            .map(|value| parse_hash(value, "withdrawals root"))
            .transpose()?,
        blob_gas_used: header
            .blob_gas_used
            .as_deref()
            .map(|value| parse_u64_hex(value, "blob gas used"))
            .transpose()?,
        excess_blob_gas: header
            .excess_blob_gas
            .as_deref()
            .map(|value| parse_u64_hex(value, "excess blob gas"))
            .transpose()?,
        parent_beacon_block_root: header
            .parent_beacon_block_root
            .as_deref()
            .map(|value| parse_hash(value, "parent beacon block root"))
            .transpose()?,
        highest_precompile_address: parse_address(
            &data.highest_precompile_address,
            "highest precompile address",
        )?,
        transaction_count: regular_count,
        system_transaction_count: system_count,
        ommers: json_array_object(&contents.body.ommers)?,
        withdrawals: json_array_object(&contents.body.withdrawals)?,
    };

    let total_count = regular_count
        .checked_add(system_count)
        .ok_or_else(|| anyhow::anyhow!("EVM transaction count overflow"))?;
    let mut transactions = Vec::with_capacity(total_count as usize);
    let mut receipts = Vec::with_capacity(total_count as usize);
    let log_capacity = data
        .receipts
        .iter()
        .map(|receipt| receipt.logs.len())
        .sum::<usize>()
        + data
            .system_txs
            .iter()
            .map(|transaction| transaction.receipt.logs.len())
            .sum::<usize>();
    let mut logs = Vec::with_capacity(log_capacity);

    let mut previous_cumulative_gas = 0;
    for (index, (transaction, receipt)) in contents
        .body
        .transactions
        .iter()
        .zip(&data.receipts)
        .enumerate()
    {
        let transaction_index =
            u32::try_from(index).map_err(|_| anyhow::anyhow!("EVM transaction index overflow"))?;
        let transaction_hash =
            signed_transaction_hash(&transaction.transaction, &transaction.signature)?;
        transactions.push(transaction_row(
            block_time,
            block_number,
            transaction_index,
            Some(transaction_hash),
            false,
            None,
            &transaction.transaction,
            Some(&transaction.signature),
        )?);
        append_receipt(
            &mut receipts,
            &mut logs,
            block_time,
            block_number,
            transaction_index,
            Some(transaction_hash),
            false,
            receipt,
            &mut previous_cumulative_gas,
        )?;
    }

    previous_cumulative_gas = 0;
    for (index, transaction) in data.system_txs.iter().enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| anyhow::anyhow!("EVM system transaction index overflow"))?;
        let transaction_index = regular_count
            .checked_add(index)
            .ok_or_else(|| anyhow::anyhow!("EVM transaction index overflow"))?;
        let from = parse_address(&transaction.from, "system transaction sender")?;
        transactions.push(transaction_row(
            block_time,
            block_number,
            transaction_index,
            None,
            true,
            Some(from),
            &transaction.tx,
            None,
        )?);
        append_receipt(
            &mut receipts,
            &mut logs,
            block_time,
            block_number,
            transaction_index,
            None,
            true,
            &transaction.receipt,
            &mut previous_cumulative_gas,
        )?;
    }

    let mut read_precompile_calls = Vec::new();
    for (precompile_index, (address, calls)) in data.read_precompile_calls.iter().enumerate() {
        let precompile_index = u32::try_from(precompile_index)
            .map_err(|_| anyhow::anyhow!("EVM precompile index overflow"))?;
        let address = parse_address(address, "read precompile address")?;
        for (call_index, (input, result)) in calls.iter().enumerate() {
            let call_index = u32::try_from(call_index)
                .map_err(|_| anyhow::anyhow!("EVM precompile call index overflow"))?;
            let (success, gas_used, output, error) = match result {
                EvmPrecompileResult::Ok(output) => (
                    true,
                    Some(output.gas_used),
                    output.bytes.clone(),
                    String::new(),
                ),
                EvmPrecompileResult::Err(error) => {
                    (false, None, String::new(), serde_json::to_string(error)?)
                }
            };
            read_precompile_calls.push(EvmReadPrecompileCallRow {
                block_time,
                block_number,
                precompile_index,
                call_index,
                address,
                input: input.input.clone(),
                gas_limit: input.gas_limit,
                success,
                gas_used,
                output,
                error,
            });
        }
    }

    Ok(EvmBlocksAndReceiptsRows {
        block,
        transactions,
        receipts,
        logs,
        read_precompile_calls,
    })
}

fn json_array_object<T: Serialize>(items: &[T]) -> anyhow::Result<String> {
    let items = serde_json::to_string(items)?;
    Ok(format!(r#"{{"items":{items}}}"#))
}

#[allow(clippy::too_many_arguments)]
fn transaction_row(
    block_time: chrono::DateTime<Utc>,
    block_number: u64,
    transaction_index: u32,
    transaction_hash: Option<Hash>,
    is_system: bool,
    from_address: Option<Address>,
    transaction: &EvmTransaction,
    signature: Option<&EvmSignature>,
) -> anyhow::Result<EvmTransactionRow> {
    let (
        transaction_type,
        chain_id,
        nonce,
        gas,
        gas_price,
        max_fee,
        max_priority_fee,
        to,
        value,
        input,
        access_list,
    ) = match transaction {
        EvmTransaction::Legacy(tx) => (
            "Legacy",
            tx.chain_id.as_deref(),
            tx.nonce.as_str(),
            tx.gas.as_str(),
            Some(tx.gas_price.as_str()),
            None,
            None,
            tx.to.as_deref(),
            tx.value.as_str(),
            tx.input.as_str(),
            r#"{"items":[]}"#.to_string(),
        ),
        EvmTransaction::Eip2930(tx) => (
            "Eip2930",
            Some(tx.chain_id.as_str()),
            tx.nonce.as_str(),
            tx.gas.as_str(),
            Some(tx.gas_price.as_str()),
            None,
            None,
            tx.to.as_deref(),
            tx.value.as_str(),
            tx.input.as_str(),
            json_array_object(&tx.access_list)?,
        ),
        EvmTransaction::Eip1559(tx) => (
            "Eip1559",
            Some(tx.chain_id.as_str()),
            tx.nonce.as_str(),
            tx.gas.as_str(),
            None,
            Some(tx.max_fee_per_gas.as_str()),
            Some(tx.max_priority_fee_per_gas.as_str()),
            tx.to.as_deref(),
            tx.value.as_str(),
            tx.input.as_str(),
            json_array_object(&tx.access_list)?,
        ),
    };

    let (signature_r, signature_s, signature_y_parity) = match signature {
        Some(signature) => (
            Some(parse_uint256(&signature.r, "transaction signature r")?),
            Some(parse_uint256(&signature.s, "transaction signature s")?),
            Some(parse_parity(signature)?),
        ),
        None => (None, None, None),
    };

    Ok(EvmTransactionRow {
        block_time,
        block_number,
        transaction_index,
        hash: transaction_hash,
        is_system,
        transaction_type: transaction_type.to_string(),
        chain_id: chain_id
            .map(|value| parse_u64_hex(value, "transaction chain id"))
            .transpose()?,
        nonce: parse_u64_hex(nonce, "transaction nonce")?,
        gas: parse_u64_hex(gas, "transaction gas")?,
        gas_price: gas_price
            .map(|value| parse_uint256(value, "transaction gas price"))
            .transpose()?,
        max_fee_per_gas: max_fee
            .map(|value| parse_uint256(value, "transaction max fee per gas"))
            .transpose()?,
        max_priority_fee_per_gas: max_priority_fee
            .map(|value| parse_uint256(value, "transaction max priority fee per gas"))
            .transpose()?,
        from_address,
        to_address: to
            .map(|value| parse_address(value, "transaction recipient"))
            .transpose()?,
        value: parse_uint256(value, "transaction value")?,
        input: input.to_string(),
        access_list,
        signature_r,
        signature_s,
        signature_y_parity,
    })
}

#[allow(clippy::too_many_arguments)]
fn append_receipt(
    receipts: &mut Vec<EvmReceiptRow>,
    logs: &mut Vec<EvmLogRow>,
    block_time: chrono::DateTime<Utc>,
    block_number: u64,
    transaction_index: u32,
    transaction_hash: Option<Hash>,
    is_system: bool,
    receipt: &EvmReceipt,
    previous_cumulative_gas: &mut u64,
) -> anyhow::Result<()> {
    let gas_used = receipt
        .cumulative_gas_used
        .checked_sub(*previous_cumulative_gas)
        .ok_or_else(|| {
            anyhow::anyhow!("receipt cumulative gas decreased in EVM block {block_number}")
        })?;
    *previous_cumulative_gas = receipt.cumulative_gas_used;
    let log_count = u32::try_from(receipt.logs.len())
        .map_err(|_| anyhow::anyhow!("EVM receipt log count overflow"))?;

    receipts.push(EvmReceiptRow {
        block_time,
        block_number,
        transaction_index,
        transaction_hash,
        is_system,
        transaction_type: receipt.tx_type.clone(),
        success: receipt.success,
        cumulative_gas_used: receipt.cumulative_gas_used,
        gas_used,
        log_count,
    });

    for (log_index, log) in receipt.logs.iter().enumerate() {
        let log_index =
            u32::try_from(log_index).map_err(|_| anyhow::anyhow!("EVM log index overflow"))?;
        let topics = log
            .topics
            .iter()
            .map(|topic| parse_hash(topic, "log topic"))
            .collect::<anyhow::Result<Vec<_>>>()?;
        logs.push(EvmLogRow {
            block_time,
            block_number,
            transaction_index,
            transaction_hash,
            is_system,
            log_index,
            address: parse_address(&log.address, "log address")?,
            topics,
            data: log.data.clone(),
        });
    }

    Ok(())
}

fn parse_u64_hex(value: &str, field: &str) -> anyhow::Result<u64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.is_empty() {
        return Err(anyhow::anyhow!("parse {field}: empty hex quantity"));
    }
    u64::from_str_radix(value, 16).map_err(|e| anyhow::anyhow!("parse {field}: {e}"))
}

fn parse_hash(value: &str, field: &str) -> anyhow::Result<Hash> {
    hash_from_hex(value).map_err(|e| anyhow::anyhow!("parse {field}: {e}"))
}

fn parse_address(value: &str, field: &str) -> anyhow::Result<Address> {
    address_from_hex(value).map_err(|e| anyhow::anyhow!("parse {field}: {e}"))
}

fn parse_uint256(value: &str, field: &str) -> anyhow::Result<Hash> {
    uint256_from_hex(value).map_err(|e| anyhow::anyhow!("parse {field}: {e}"))
}

fn parse_parity(signature: &EvmSignature) -> anyhow::Result<u8> {
    let parity = parse_u64_hex(&signature.y_parity, "transaction signature parity")?;
    if parity > 1 {
        return Err(anyhow::anyhow!(
            "transaction signature parity must be 0 or 1"
        ));
    }
    Ok(parity as u8)
}

fn signed_transaction_hash(
    transaction: &EvmTransaction,
    signature: &EvmSignature,
) -> anyhow::Result<Hash> {
    let parity = parse_parity(signature)?;
    let r = rlp_quantity(&signature.r)?;
    let s = rlp_quantity(&signature.s)?;

    let encoded = match transaction {
        EvmTransaction::Legacy(tx) => {
            let v = match tx.chain_id.as_deref() {
                Some(chain_id) => parse_u64_hex(chain_id, "legacy transaction chain id")?
                    .checked_mul(2)
                    .and_then(|value| value.checked_add(35 + u64::from(parity)))
                    .ok_or_else(|| anyhow::anyhow!("legacy transaction v overflow"))?,
                None => 27 + u64::from(parity),
            };
            rlp_list(&[
                rlp_quantity(&tx.nonce)?,
                rlp_quantity(&tx.gas_price)?,
                rlp_quantity(&tx.gas)?,
                rlp_optional_address(tx.to.as_deref())?,
                rlp_quantity(&tx.value)?,
                rlp_hex_data(&tx.input)?,
                rlp_u64(v),
                r,
                s,
            ])
        }
        EvmTransaction::Eip2930(tx) => {
            let mut out = vec![0x01];
            out.extend(rlp_list(&[
                rlp_quantity(&tx.chain_id)?,
                rlp_quantity(&tx.nonce)?,
                rlp_quantity(&tx.gas_price)?,
                rlp_quantity(&tx.gas)?,
                rlp_optional_address(tx.to.as_deref())?,
                rlp_quantity(&tx.value)?,
                rlp_hex_data(&tx.input)?,
                rlp_access_list(&tx.access_list)?,
                rlp_u64(u64::from(parity)),
                r,
                s,
            ]));
            out
        }
        EvmTransaction::Eip1559(tx) => {
            let mut out = vec![0x02];
            out.extend(rlp_list(&[
                rlp_quantity(&tx.chain_id)?,
                rlp_quantity(&tx.nonce)?,
                rlp_quantity(&tx.max_priority_fee_per_gas)?,
                rlp_quantity(&tx.max_fee_per_gas)?,
                rlp_quantity(&tx.gas)?,
                rlp_optional_address(tx.to.as_deref())?,
                rlp_quantity(&tx.value)?,
                rlp_hex_data(&tx.input)?,
                rlp_access_list(&tx.access_list)?,
                rlp_u64(u64::from(parity)),
                r,
                s,
            ]));
            out
        }
    };

    Ok(hash(&encoded))
}

fn rlp_access_list(access_list: &[EvmAccessListItem]) -> anyhow::Result<Vec<u8>> {
    let mut items = Vec::with_capacity(access_list.len());
    for item in access_list {
        let address = address_from_hex(&item.address)
            .map_err(|e| anyhow::anyhow!("parse access list address: {e}"))?;
        let storage_keys = item
            .storage_keys
            .iter()
            .map(|key| {
                hash_from_hex(key)
                    .map(|key| rlp_bytes(&key))
                    .map_err(|e| anyhow::anyhow!("parse access list storage key: {e}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        items.push(rlp_list(&[rlp_bytes(&address), rlp_list(&storage_keys)]));
    }
    Ok(rlp_list(&items))
}

fn rlp_optional_address(address: Option<&str>) -> anyhow::Result<Vec<u8>> {
    match address {
        Some(address) => Ok(rlp_bytes(&address_from_hex(address).map_err(|e| {
            anyhow::anyhow!("parse transaction recipient for hash: {e}")
        })?)),
        None => Ok(rlp_bytes(&[])),
    }
}

fn rlp_quantity(value: &str) -> anyhow::Result<Vec<u8>> {
    let value = sig_from_hex(value).map_err(|e| anyhow::anyhow!("parse RLP quantity: {e}"))?;
    let first_nonzero = value
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(value.len());
    Ok(rlp_bytes(&value[first_nonzero..]))
}

fn rlp_u64(value: u64) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let first_nonzero = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len());
    rlp_bytes(&bytes[first_nonzero..])
}

fn rlp_hex_data(value: &str) -> anyhow::Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        return Err(anyhow::anyhow!("hex data has an odd length"));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks(2) {
        let hi = (chunk[0] as char)
            .to_digit(16)
            .ok_or_else(|| anyhow::anyhow!("invalid hex data"))?;
        let lo = (chunk[1] as char)
            .to_digit(16)
            .ok_or_else(|| anyhow::anyhow!("invalid hex data"))?;
        bytes.push(((hi << 4) | lo) as u8);
    }
    Ok(rlp_bytes(&bytes))
}

fn rlp_bytes(bytes: &[u8]) -> Vec<u8> {
    if bytes.len() == 1 && bytes[0] < 0x80 {
        return bytes.to_vec();
    }
    let mut out = rlp_prefix(0x80, 0xb7, bytes.len());
    out.extend_from_slice(bytes);
    out
}

fn rlp_list(items: &[Vec<u8>]) -> Vec<u8> {
    let payload_len = items.iter().map(Vec::len).sum();
    let mut out = rlp_prefix(0xc0, 0xf7, payload_len);
    for item in items {
        out.extend_from_slice(item);
    }
    out
}

fn rlp_prefix(short_offset: u8, long_offset: u8, len: usize) -> Vec<u8> {
    if len < 56 {
        return vec![short_offset + len as u8];
    }
    let bytes = len.to_be_bytes();
    let first_nonzero = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len());
    let len_bytes = &bytes[first_nonzero..];
    let mut out = Vec::with_capacity(1 + len_bytes.len());
    out.push(long_offset + len_bytes.len() as u8);
    out.extend_from_slice(len_bytes);
    out
}

pub struct EvmBlocksAndReceipts {
    blocks: Inserter<EvmBlockRow>,
    transactions: Inserter<EvmTransactionRow>,
    receipts: Inserter<EvmReceiptRow>,
    logs: Inserter<EvmLogRow>,
    read_precompile_calls: Inserter<EvmReadPrecompileCallRow>,
}

impl EvmBlocksAndReceipts {
    pub fn new(ch: &Client) -> Self {
        Self {
            blocks: new_inserter(ch, "evm_blocks"),
            transactions: new_inserter(ch, "evm_transactions"),
            receipts: new_inserter(ch, "evm_receipts"),
            logs: new_inserter(ch, "evm_logs"),
            read_precompile_calls: new_inserter(ch, "evm_read_precompile_calls"),
        }
    }
}

impl Stream for EvmBlocksAndReceipts {
    const NAME: &'static str = "evm_blocks_and_receipts";
    const SOURCE_DIR: &'static str = "evm_block_and_receipts";

    type Rows = EvmBlocksAndReceiptsRows;

    fn parse(event: Event) -> anyhow::Result<Self::Rows> {
        parse(event.line)
    }

    async fn write(&mut self, rows: &Self::Rows, metrics: &Metrics) -> anyhow::Result<()> {
        let lag = (Utc::now() - rows.block.block_time).as_seconds_f64();
        metrics.record_lag(Self::NAME, lag);
        metrics.record_ingested(Self::NAME, "block");
        self.blocks.write(&rows.block).await?;

        for transaction in &rows.transactions {
            metrics.record_ingested(Self::NAME, "transaction");
            self.transactions.write(transaction).await?;
        }
        for receipt in &rows.receipts {
            metrics.record_ingested(Self::NAME, "receipt");
            self.receipts.write(receipt).await?;
        }
        for log in &rows.logs {
            metrics.record_ingested(Self::NAME, "log");
            self.logs.write(log).await?;
        }
        for call in &rows.read_precompile_calls {
            metrics.record_ingested(Self::NAME, "read_precompile_call");
            self.read_precompile_calls.write(call).await?;
        }

        Ok(())
    }

    async fn commit(&mut self, metrics: &Metrics, force: bool) -> anyhow::Result<()> {
        commit_metered(&mut self.blocks, "evm_blocks", metrics, force).await?;
        commit_metered(&mut self.transactions, "evm_transactions", metrics, force).await?;
        commit_metered(&mut self.receipts, "evm_receipts", metrics, force).await?;
        commit_metered(&mut self.logs, "evm_logs", metrics, force).await?;
        commit_metered(
            &mut self.read_precompile_calls,
            "evm_read_precompile_calls",
            metrics,
            force,
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_evm_block() {
        let line = br#"["2026-07-14T05:00:00.937379506",{"block":{"Reth115":{"header":{"hash":"0xef59b0e433e52825cfd278f049260e19240d1e08319576e15b0a6503722f815e","header":{"parentHash":"0x40bc9f5bcc0d2df9d6fb9882ae8bf17c12ecd92a542df41843a90414ded7e796","sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347","miner":"0x0000000000000000000000000000000000000000","stateRoot":"0x0000000000000000000000000000000000000000000000000000000000000000","transactionsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421","receiptsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421","logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","difficulty":"0x0","number":"0x268a327","gasLimit":"0x2dc6c0","gasUsed":"0x0","timestamp":"0x6a55c250","extraData":"0x","mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000","nonce":"0x0000000000000000","baseFeePerGas":"0x5f5e100","withdrawalsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421","blobGasUsed":"0x0","excessBlobGas":"0x0","parentBeaconBlockRoot":"0x0000000000000000000000000000000000000000000000000000000000000000"}},"body":{"transactions":[],"ommers":[],"withdrawals":[]}}},"receipts":[],"system_txs":[],"read_precompile_calls":[],"highest_precompile_address":"0x0000000000000000000000000000000000000813"}]"#;

        let rows = parse(line.to_vec()).expect("EVM block should parse");
        assert_eq!(rows.block.block_number, 40_411_943);
        assert_eq!(rows.block.transaction_count, 0);
        assert!(rows.transactions.is_empty());
        assert!(rows.receipts.is_empty());
        assert_eq!(rows.block.ommers, r#"{"items":[]}"#);
        assert_eq!(rows.block.withdrawals, r#"{"items":[]}"#);
    }

    #[test]
    fn parses_populated_evm_block() {
        let line = serde_json::to_vec(&serde_json::json!([
            "2026-07-14T05:00:00.000000001",
            {
                "block": {
                    "Reth115": {
                        "header": {
                            "hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                            "header": {
                                "parentHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                                "sha3Uncles": "0x3333333333333333333333333333333333333333333333333333333333333333",
                                "miner": "0x4444444444444444444444444444444444444444",
                                "stateRoot": "0x5555555555555555555555555555555555555555555555555555555555555555",
                                "transactionsRoot": "0x6666666666666666666666666666666666666666666666666666666666666666",
                                "receiptsRoot": "0x7777777777777777777777777777777777777777777777777777777777777777",
                                "logsBloom": "0x00",
                                "difficulty": "0x0",
                                "number": "0x1",
                                "gasLimit": "0x100000",
                                "gasUsed": "0x5208",
                                "timestamp": "0x1",
                                "extraData": "0x",
                                "mixHash": "0x8888888888888888888888888888888888888888888888888888888888888888",
                                "nonce": "0x0"
                            }
                        },
                        "body": {
                            "transactions": [{
                                "signature": {
                                    "r": "0x1",
                                    "s": "0x2",
                                    "yParity": "0x0",
                                    "v": "0x0"
                                },
                                "transaction": {
                                    "Eip1559": {
                                        "chainId": "0x3e7",
                                        "nonce": "0x1",
                                        "gas": "0x5208",
                                        "maxFeePerGas": "0x5f5e100",
                                        "maxPriorityFeePerGas": "0x0",
                                        "to": null,
                                        "value": "0x0",
                                        "accessList": [],
                                        "input": "0x"
                                    }
                                }
                            }],
                            "ommers": [],
                            "withdrawals": []
                        }
                    }
                },
                "receipts": [{
                    "tx_type": "Eip1559",
                    "success": true,
                    "cumulative_gas_used": 21000,
                    "logs": [{
                        "address": "0x9999999999999999999999999999999999999999",
                        "topics": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "data": "0x01"
                    }]
                }],
                "system_txs": [{
                    "tx": {
                        "Legacy": {
                            "chainId": "0x3e7",
                            "nonce": "0x2",
                            "gasPrice": "0x0",
                            "gas": "0x100",
                            "to": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                            "value": "0x0",
                            "input": "0x"
                        }
                    },
                    "receipt": {
                        "tx_type": "Legacy",
                        "success": false,
                        "cumulative_gas_used": 100,
                        "logs": []
                    },
                    "from": "0xcccccccccccccccccccccccccccccccccccccccc"
                }],
                "read_precompile_calls": [[
                    "0x0000000000000000000000000000000000000800",
                    [
                        [{"input": "0x01", "gas_limit": 1000}, {"Ok": {"gas_used": 10, "bytes": "0x02"}}],
                        [{"input": "0x03", "gas_limit": 2000}, {"Err": {"reason": "failed"}}]
                    ]
                ]],
                "highest_precompile_address": "0x0000000000000000000000000000000000000813"
            }
        ]))
        .unwrap();

        let rows = parse(line).expect("populated EVM block should parse");
        assert_eq!(rows.block.block_number, 1);
        assert_eq!(rows.block.transaction_count, 1);
        assert_eq!(rows.block.system_transaction_count, 1);
        assert_eq!(rows.transactions.len(), 2);
        assert!(rows.transactions[0].hash.is_some());
        assert!(rows.transactions[1].hash.is_none());
        assert_eq!(rows.transactions[0].access_list, r#"{"items":[]}"#);
        assert_eq!(rows.transactions[1].access_list, r#"{"items":[]}"#);
        assert_eq!(rows.receipts[0].gas_used, 21_000);
        assert_eq!(rows.receipts[1].gas_used, 100);
        assert_eq!(rows.logs.len(), 1);
        assert_eq!(rows.read_precompile_calls.len(), 2);
        assert!(rows.read_precompile_calls[0].success);
        assert!(!rows.read_precompile_calls[1].success);
        assert_eq!(
            rows.read_precompile_calls[1].error,
            r#"{"reason":"failed"}"#
        );
    }

    #[test]
    fn computes_transaction_hashes_for_supported_envelopes() {
        let cases = [
            (
                r#"{"signature":{"r":"0x21674c32b29445af44fce3ed57e9f956518f6df5dc154da079803ec2e7bb8cfc","s":"0x337c1c0ae1c959db5378ddbf7570f9cc1290075f1aa5bfea5930cbe83cc96838","yParity":"0x1","v":"0x1"},"transaction":{"Legacy":{"chainId":"0x3e7","nonce":"0x3502","gasPrice":"0x5f5e100","gas":"0x7530","to":"0x9106d218bcfe8e17b8c900eea3e89352b94ac054","value":"0x55a28736b6292","input":"0x"}}}"#,
                "0xcacdaba149b5f03b65f3c7bdf64d97cacc324b9a6129bc67136fbfb9a138b0a0",
            ),
            (
                r#"{"signature":{"r":"0xfe52453e376643033ba3672bdd1c2447dd1c6ec57911448e3385700a0dfd4cb7","s":"0x33dcee22d1d514d3a7a0ab300314796d25b7b50460a31a0c265bd4101751c9cd","yParity":"0x1","v":"0x1"},"transaction":{"Eip2930":{"chainId":"0x3e7","nonce":"0x473fc","gasPrice":"0x3a3adfc4","gas":"0x5208","to":"0x75fca5d6ecb27d37076ccd15b1ac837b5ffeddc9","value":"0x0","accessList":[],"input":"0x"}}}"#,
                "0xf5d3ee789e15de55c863dcb58f0d2e364a308ef11bb718d1ce17d9e96c09f97e",
            ),
            (
                r#"{"signature":{"r":"0x6d3a5623357ee67101f44e572d6bf5e80b070f8cb3b919cb261532bb8da633a0","s":"0x62138d553d8afcf8e25e50a9effc8ec6d44ead09431aa50929e5a615b6a197e8","yParity":"0x1","v":"0x1"},"transaction":{"Eip1559":{"chainId":"0x3e7","nonce":"0x3571","gas":"0x5208","maxFeePerGas":"0xbebc200","maxPriorityFeePerGas":"0x2faf080","to":"0x6a00d5263aa85ba9dcc0ce30424251b189226e56","value":"0x9184e72a000","accessList":[],"input":"0x"}}}"#,
                "0x36e6ae381d8ae3c15dea77b142c0c2715c61deda1a3379cf0271c0efdb78c80a",
            ),
        ];

        for (json, expected_hash) in cases {
            let signed: types::EvmSignedTransaction = serde_json::from_str(json).unwrap();
            let actual = signed_transaction_hash(&signed.transaction, &signed.signature).unwrap();
            assert_eq!(actual, hash_from_hex(expected_hash).unwrap());
        }
    }

    #[test]
    fn rlp_encodes_canonical_examples() {
        assert_eq!(rlp_bytes(&[]), vec![0x80]);
        assert_eq!(rlp_bytes(b"dog"), vec![0x83, b'd', b'o', b'g']);
        assert_eq!(
            rlp_list(&[rlp_bytes(b"cat"), rlp_bytes(b"dog")]),
            b"\xc8\x83cat\x83dog"
        );
    }
}
