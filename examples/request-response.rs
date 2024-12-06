use bytes::Bytes;
use futures::StreamExt;

use rust_ipfs::{p2p::RequestResponseConfig, UninitializedIpfsDefault as UninitializedIpfs};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let node_a = UninitializedIpfs::new()
        .with_default()
        .add_listening_addr("/ip4/127.0.0.1/tcp/0".parse()?)
        .with_request_response(vec![RequestResponseConfig {
            protocol: "/ping/0".into(),
            ..Default::default()
        }])
        .start()
        .await?;

    let node_b = UninitializedIpfs::new()
        .with_default()
        .add_listening_addr("/ip4/127.0.0.1/tcp/0".parse()?)
        .with_request_response(vec![RequestResponseConfig {
            protocol: "/ping/0".into(),
            ..Default::default()
        }])
        .start()
        .await?;

    let peer_id = node_a.keypair().public().to_peer_id();
    let peer_id_b = node_b.keypair().public().to_peer_id();

    println!("Our Node (A): {peer_id}");
    println!("Their Node (B): {peer_id_b}");

    let addrs = node_a.listening_addresses().await?;

    for addr in addrs {
        node_b.add_peer((peer_id, addr)).await?;
    }

    let mut node_a_st = node_a.requests_subscribe(()).await?;

    tokio::spawn({
        let node_a = node_a.clone();
        async move {
            let Some((pid, id, request)) = node_a_st.next().await else {
                return;
            };
            let res_str = String::from_utf8_lossy(&request);
            println!("{pid} requested {res_str} from {peer_id}");
            let res = Bytes::copy_from_slice(b"pong");
            node_a.send_response(pid, id, res).await.expect("msg")
        }
    });

    let response = node_b.send_request(peer_id, b"ping").await?;

    println!(
        "{peer_id} responded with {}",
        String::from_utf8_lossy(&response)
    );

    node_a.exit_daemon().await;
    node_b.exit_daemon().await;
    Ok(())
}
