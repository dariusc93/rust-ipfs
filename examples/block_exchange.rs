use std::collections::VecDeque;
// use std::sync::Arc;
// use std::time::Duration;

use rand::RngCore;
use rust_ipfs::{Ipfs, Keypair};
use web_time::Duration;

// use tokio::sync::Barrier;

fn generate_ed25519(secret_key_seed: u8) -> Keypair {
    let mut bytes = [0u8; 32];
    bytes[0] = secret_key_seed;

    Keypair::ed25519_from_bytes(bytes).expect("only errors on wrong length")
}

pub async fn spawn_nodes<const N: usize>() -> VecDeque<Ipfs> {
    use rust_ipfs::Node;
    let mut nodes = Vec::with_capacity(N);

    for i in 0..N {
        let kp = generate_ed25519(i as _);
        let node = Node::with_options(None, kp).await;
        nodes.push(node);
    }

    // for i in 0..N {
    //     for (j, peer) in nodes.iter().enumerate() {
    //         if i != j {
    //             //TODO: Determine the cause of `Failed to negotiate transport protocol(s).... Cannot assign requested address (os error 99): Cannot assign requested address (os error 99)`
    //             if let Err(_e) = nodes[i].connect(peer.addrs[0].clone()).await {}
    //         }
    //     }
    // }

    for i in 0..(N - 1) {
        nodes[i]
            .connect(nodes[i + 1].addrs[0].clone())
            .await
            .unwrap();
    }

    nodes.into_iter().map(|n| n.ipfs).collect::<VecDeque<_>>()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let mut nodes = spawn_nodes::<3>().await;

    let mainnode = nodes.pop_front().unwrap();
    let _provider = mainnode.keypair().public().to_peer_id();
    let mut handles = Vec::with_capacity(nodes.len());
    // let barrier = Arc::new(Barrier::new(nodes.len()));

    let mut rng = rand::thread_rng();

    let mut data = vec![0u8; 2 * 1024 * 1024];

    rng.fill_bytes(&mut data);

    // let data = "abcde".repeat(1024 * 1024).as_bytes().to_vec();
    let path = mainnode.add_unixfs(data).await?;

    while let Some(node) = nodes.pop_front() {
        // let c = barrier.clone();
        let peer_id = node.keypair().public().to_peer_id();
        // let provider = next_provider.replace(peer_id).expect("valid provider");
        let path = path.clone();

        handles.push(tokio::spawn(async move {
            // println!("{peer_id} is waiting");
            //  let _ = c.wait().await;
            println!("{peer_id} is fetching {path}");
            let start = std::time::Instant::now();
            let _data = node
                .cat_unixfs(path)
                // .provider(provider)
                .await
                .map_err(|e| {
                    println!("{} failed", peer_id);
                    e
                })
                .unwrap();
            let end = start.elapsed();
            println!("{peer_id} took {}ms to complete", end.as_millis());
        }))
    }

    // let data = "abde".repeat(5 * 1024 * 1024).as_bytes().to_vec();
    // mainnode.add_unixfs(data).await?;

    for handle in handles {
        handle.await.unwrap();
    }

    // tokio::time::sleep(Duration::from_secs(10)).await;

    Ok(())
}
