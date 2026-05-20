//! Update flow-router config PDA to fee_bps=0 for testing (skip fee collection).
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   cargo test --test update_config_zero_fee -- --nocapture
//! ```

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use std::str::FromStr;

const STAGING_ROUTER: &str = "FLorgvPfcfirXaKvuFTDgeqZoDAMXAymZcnfNDK4mmGj";

#[tokio::test]
async fn test_update_config_zero_fee() {
    let rpc_url = std::env::var("RPC_URL")
        .or_else(|_| std::env::var("SOL_HTTPS_ENDPOINT"))
        .expect("RPC_URL");
    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    let admin_bytes: Vec<u8> = serde_json::from_str(
        &std::fs::read_to_string("/tmp/deployer-keypair.json").unwrap()
    ).unwrap();
    let admin = Keypair::from_bytes(&admin_bytes).unwrap();

    let program_id = Pubkey::from_str(STAGING_ROUTER).unwrap();
    let (config_pda, _) = Pubkey::find_program_address(&[b"config"], &program_id);

    eprintln!("\n=== UPDATE CONFIG: fee_bps=50, protocol_fee=WSOL ATA ===\n");
    eprintln!("  Admin:      {}", admin.pubkey());
    eprintln!("  Config PDA: {}", config_pda);

    // Data: [3u8] + fee_bps(u16 LE) + protocol_fee_account(32) + referral_split_bps(u16 LE)
    let fee_bps: u16 = 50; // 0.5% platform fee
    // Real WSOL token account for protocol fees
    let protocol_fee_account = Pubkey::from_str("6QqWD4pWu5YhMfMPdWpa9QSDC3qUyXHLvHeLKvCsJ3dy").unwrap();
    let referral_split_bps: u16 = 7000;

    let mut data = Vec::with_capacity(37);
    data.push(3u8); // UPDATE_CONFIG_DISC
    data.extend_from_slice(&fee_bps.to_le_bytes());
    data.extend_from_slice(&protocol_fee_account.to_bytes());
    data.extend_from_slice(&referral_split_bps.to_le_bytes());

    let ix = Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new_readonly(admin.pubkey(), true), // admin (signer)
            AccountMeta::new(config_pda, false),              // config_pda (writable)
        ],
        data,
    };

    let priority = ComputeBudgetInstruction::set_compute_unit_price(10000);
    let blockhash = rpc.get_latest_blockhash().await.unwrap();
    let tx = Transaction::new_signed_with_payer(
        &[priority, ix], Some(&admin.pubkey()), &[&admin], blockhash,
    );

    match rpc.send_and_confirm_transaction(&tx).await {
        Ok(sig) => {
            eprintln!("  [OK] Config updated: fee_bps=0. Sig: {}", sig);

            // Verify
            let acct = rpc.get_account(&config_pda).await.unwrap();
            let stored_fee = u16::from_le_bytes(acct.data[40..42].try_into().unwrap());
            assert_eq!(stored_fee, 50);
            let stored_protocol = Pubkey::new_from_array(acct.data[42..74].try_into().unwrap());
            eprintln!("  [OK] Verified on-chain: fee_bps={}, protocol_fee={}", stored_fee, stored_protocol);
        }
        Err(e) => panic!("Failed: {}", e),
    }
}
