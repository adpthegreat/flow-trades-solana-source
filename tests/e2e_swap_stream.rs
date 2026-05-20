//! End-to-end tests for the swap-stream endpoint.
//!
//! Two scenarios:
//! 1. **Disabled mode** — server boots with `--swap-stream-enabled=false`;
//!    `/swap-stream` accepts the upgrade then immediately sends an error
//!    frame and disconnects.
//! 2. **Enabled mode + protocol round-trip** — server boots with the
//!    swap stream on; client connects, sends a subscribe filter, gets
//!    `{"type":"subscribed"}` back, and then sees a server ping within
//!    the keep-alive window. Whether actual `swap` frames arrive
//!    depends on Geyser activity (not reachable from this VM); we
//!    accept either ping or swap as a healthy frame.
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   cargo test --test e2e_swap_stream -- --nocapture --test-threads=1
//! ```

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMsg;

/// Allocate a unique workdir per test so they can run in parallel without
/// stomping on each other's pool databases.
fn unique_workdir() -> PathBuf {
    static N: AtomicU16 = AtomicU16::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let path = std::env::temp_dir().join(format!("flow-trades-swap-stream-{}-{}", pid, n));
    std::fs::create_dir_all(&path).expect("mkdir tempdir");
    path
}

const PORT_BASE: u16 = 18091;

fn next_port() -> u16 {
    static OFFSET: AtomicU16 = AtomicU16::new(0);
    PORT_BASE + OFFSET.fetch_add(1, Ordering::Relaxed)
}

fn rpc_url() -> String {
    std::env::var("RPC_URL")
        .or_else(|_| std::env::var("SOL_HTTPS_ENDPOINT"))
        .expect("RPC_URL required")
}

fn binary_path() -> String {
    // Tests are run from the crate root; cargo test --release uses target/release.
    let path = std::env::var("CARGO_BIN_EXE_flow-trades").ok();
    if let Some(p) = path {
        return p;
    }
    // Fallback: assume target/release.
    "target/release/flow-trades".to_string()
}

struct Server {
    child: Child,
    workdir: PathBuf,
    port: u16,
}

impl Server {
    fn spawn(swap_stream_enabled: bool) -> Self {
        let port = next_port();
        let workdir = unique_workdir();
        let pool_db = workdir.join("pools.db");
        let warm = workdir.join("pool-state.bin");
        let snap = workdir.join("pool-snapshot.json");

        let child = Command::new(binary_path())
            .args([
                "--rpc-url",
                &rpc_url(),
                "--listen",
                &format!("127.0.0.1:{port}"),
                "--log-level",
                "warn",
                "--pool-db-path",
                pool_db.to_str().unwrap(),
                "--warm-storage-path",
                warm.to_str().unwrap(),
                "--snapshot-path",
                snap.to_str().unwrap(),
                "--discovery-mode",
                "none",
                "--block-scan-enabled",
                "false",
                "--swap-stream-enabled",
                if swap_stream_enabled { "true" } else { "false" },
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn flow-trades");
        Self { child, workdir, port }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.workdir);
    }
}

async fn wait_for_listening(addr: &str) -> bool {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn test_swap_stream_disabled_mode_returns_error_frame() {
    let server = Server::spawn(/* enabled */ false);
    let port = server.port;
    assert!(
        wait_for_listening(&format!("127.0.0.1:{port}")).await,
        "server did not start in disabled mode"
    );

    let url = format!("ws://127.0.0.1:{port}/swap-stream");
    let (mut ws, _) = connect_async(&url).await.expect("ws connect");

    // First server frame should be the error-and-disconnect.
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for server frame")
        .expect("no frame received");
    let frame = frame.expect("ws read error");

    let text = match frame {
        WsMsg::Text(t) => t,
        other => panic!("expected text, got {other:?}"),
    };
    let v: serde_json::Value = serde_json::from_str(&text).expect("json parse");
    eprintln!("disabled-mode frame: {}", v);
    assert!(
        v.get("error")
            .and_then(|e| e.as_str())
            .map(|s| s.contains("disabled"))
            .unwrap_or(false),
        "expected error frame to mention 'disabled', got: {v}"
    );
}

#[tokio::test]
async fn test_swap_stream_enabled_protocol_handshake() {
    let server = Server::spawn(/* enabled */ true);
    let port = server.port;
    assert!(
        wait_for_listening(&format!("127.0.0.1:{port}")).await,
        "server did not start in enabled mode"
    );

    let url = format!("ws://127.0.0.1:{port}/swap-stream");
    let (mut ws, _) = connect_async(&url).await.expect("ws connect");

    // Send a subscribe filter.
    let sub = r#"{"type":"subscribe","filter":{"dex":["Pumpup Bonding","Orca"]}}"#;
    ws.send(WsMsg::Text(sub.into())).await.expect("send subscribe");

    // Expect {"type":"subscribed"} as the first server frame.
    let first = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for first frame")
        .expect("server closed unexpectedly")
        .expect("ws error");
    let text = match first {
        WsMsg::Text(t) => t,
        other => panic!("expected text frame, got {other:?}"),
    };
    let v: serde_json::Value = serde_json::from_str(&text).expect("json parse");
    eprintln!("first frame: {}", v);
    assert_eq!(v["type"], "subscribed", "expected subscribed, got {v}");

    // Try to read another frame within 35s — should be a server ping (every 30s)
    // OR a swap (if Geyser is reachable from this host). Either is healthy.
    let second = tokio::time::timeout(Duration::from_secs(35), ws.next()).await;
    match second {
        Ok(Some(Ok(WsMsg::Text(t)))) => {
            let v: serde_json::Value = serde_json::from_str(&t).expect("json parse");
            let kind = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
            eprintln!("second frame type: {}", kind);
            assert!(
                matches!(kind, "ping" | "swap" | "lagged"),
                "expected ping/swap/lagged, got: {v}"
            );
        }
        Ok(Some(Ok(other))) => {
            // Pong/binary frames are also acceptable as a sign the connection is alive.
            eprintln!("non-text but live frame: {:?}", other);
        }
        Ok(Some(Err(e))) => panic!("ws error during stream: {e}"),
        Ok(None) => panic!("connection closed unexpectedly"),
        Err(_) => panic!("no frame within 35s — server keep-alive ping not firing"),
    }

    let _ = ws.close(None).await;
}
