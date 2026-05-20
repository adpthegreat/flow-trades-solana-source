//! One-shot: Initialize the flow-router config PDA on mainnet.
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   ROUTER_PROGRAM_ID=FLorgvPfcfirXaKvuFTDgeqZoDAMXAymZcnfNDK4mmGj \
//!   cargo test --test deploy_init_config -- --nocapture
//! ```

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    system_program,
    transaction::Transaction,
};
use std::str::FromStr;

fn load_keypair() -> Keypair {
    let data = std::fs::read_to_string("/tmp/deployer-keypair.json").expect("read deployer keypair");
    let bytes: Vec<u8> = serde_json::from_str(&data).expect("parse keypair");
    Keypair::from_bytes(&bytes).expect("invalid keypair")
}

#[tokio::test]
async fn test_initialize_config() {
    let rpc_url = std::env::var("RPC_URL")
        .or_else(|_| std::env::var("SOL_HTTPS_ENDPOINT"))
        .expect("RPC_URL required");
    let program_id_str = std::env::var("ROUTER_PROGRAM_ID")
        .unwrap_or_else(|_| "FLorgvPfcfirXaKvuFTDgeqZoDAMXAymZcnfNDK4mmGj".to_string());

    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    let admin = load_keypair();
    let program_id = Pubkey::from_str(&program_id_str).unwrap();
    let (config_pda, _bump) = Pubkey::find_program_address(&[b"config"], &program_id);

    eprintln!("\n=== INITIALIZE FLOW-ROUTER CONFIG PDA ===\n");
    eprintln!("  Admin:              {}", admin.pubkey());
    eprintln!("  Program:            {}", program_id);
    eprintln!("  Config PDA:         {}", config_pda);

    // Check if already initialized
    match rpc.get_account(&config_pda).await {
        Ok(acct) => {
            if acct.data.len() >= 76 && &acct.data[..8] == b"flowconf" {
                eprintln!("  Config PDA already initialized ({} bytes)", acct.data.len());
                eprintln!("  Stored admin:       {}", Pubkey::new_from_array(acct.data[8..40].try_into().unwrap()));
                let fee_bps = u16::from_le_bytes(acct.data[40..42].try_into().unwrap());
                let protocol_fee = Pubkey::new_from_array(acct.data[42..74].try_into().unwrap());
                let referral_split = u16::from_le_bytes(acct.data[74..76].try_into().unwrap());
                eprintln!("  fee_bps:            {} ({}%)", fee_bps, fee_bps as f64 / 100.0);
                eprintln!("  protocol_fee_acct:  {}", protocol_fee);
                eprintln!("  referral_split_bps: {} ({}%)", referral_split, referral_split as f64 / 100.0);
                eprintln!("\n  [OK] Already initialized — skipping");
                return;
            }
        }
        Err(_) => {
            eprintln!("  Config PDA not found — will initialize");
        }
    }

    let fee_bps: u16 = 50; // 0.5%
    let referral_split_bps: u16 = 7000; // 70% integrator
    let protocol_fee_account = admin.pubkey(); // placeholder — use admin wallet for now

    eprintln!("  fee_bps:            {} ({}%)", fee_bps, fee_bps as f64 / 100.0);
    eprintln!("  referral_split_bps: {} ({}%)", referral_split_bps, referral_split_bps as f64 / 100.0);
    eprintln!("  protocol_fee_acct:  {} (admin wallet as placeholder)", protocol_fee_account);

    // Build init_config instruction
    // Data: [2u8] + admin(32) + fee_bps(u16 LE) + protocol_fee_account(32) + referral_split_bps(u16 LE)
    let mut data = Vec::with_capacity(69);
    data.push(2u8); // INIT_CONFIG_DISC
    data.extend_from_slice(&admin.pubkey().to_bytes());
    data.extend_from_slice(&fee_bps.to_le_bytes());
    data.extend_from_slice(&protocol_fee_account.to_bytes());
    data.extend_from_slice(&referral_split_bps.to_le_bytes());

    let ix = Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new(admin.pubkey(), true),
            AccountMeta::new(config_pda, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ],
        data,
    };

    let priority = ComputeBudgetInstruction::set_compute_unit_price(10000);
    let cu_limit = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let blockhash = rpc.get_latest_blockhash().await.expect("blockhash");
    let tx = Transaction::new_signed_with_payer(
        &[priority, cu_limit, ix],
        Some(&admin.pubkey()),
        &[&admin],
        blockhash,
    );

    eprintln!("\n  Sending transaction...");

    match rpc.send_and_confirm_transaction(&tx).await {
        Ok(sig) => {
            eprintln!("  [OK] Config initialized! Signature: {}", sig);

            // Verify
            let acct = rpc.get_account(&config_pda).await.expect("read config");
            assert_eq!(&acct.data[..8], b"flowconf");
            let stored_admin = Pubkey::new_from_array(acct.data[8..40].try_into().unwrap());
            assert_eq!(stored_admin, admin.pubkey());
            let stored_fee = u16::from_le_bytes(acct.data[40..42].try_into().unwrap());
            assert_eq!(stored_fee, fee_bps);
            eprintln!("  [OK] Config verified on-chain");
        }
        Err(e) => {
            eprintln!("  [FAIL] {}", e);
            panic!("Failed to initialize config: {}", e);
        }
    }
}
