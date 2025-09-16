use ipld_core::cid::Cid;

mod common;
use common::{spawn_nodes, Topology};
use rust_ipfs::block::BlockCodec;
use rust_ipfs::Block;

// this test is designed to trigger unfavorable conditions for the bitswap
// protocol by putting blocks in every second node and attempting to get
// them from the other nodes; intended to be used for debugging or stress
// testing the bitswap protocol (though it would be advised to uncomment
// the tracing_subscriber for stress-testing purposes)
#[ignore]
#[tokio::test]
async fn bitswap_stress_test() {
    use multihash_codetable::{Code, MultihashDigest};
    fn filter(i: usize) -> bool {
        i % 2 == 0
    }

    tracing_subscriber::fmt::init();

    let data = b"hello block\n".to_vec();
    let cid = Cid::new_v1(BlockCodec::Raw.into(), Code::Sha2_256.digest(&data));

    let nodes = spawn_nodes::<5>(Topology::Mesh).await;

    let block = Block::new(cid, data.clone()).unwrap();

    for (i, node) in nodes.iter().enumerate() {
        if filter(i) {
            node.put_block(&block).await.unwrap();
        }
    }

    for (i, node) in nodes.iter().enumerate() {
        if !filter(i) {
            node.get_block(cid).await.unwrap();
        }
    }
}
