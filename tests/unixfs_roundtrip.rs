use rust_ipfs::unixfs::ll::file::adder::Chunker;
use rust_ipfs::Node;

/// Deterministic, non-repeating payload so a wrong-order reassembly is detectable.
fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(2_654_435_761).rotate_left(13) as u8)
        .collect()
}

/// A tiny chunker turns a modest payload into a many-leaf, multi-level DAG, which forces the
/// prefetch window and reorder buffer to handle blocks completing out of consumption order.
async fn add_multiblock(node: &Node, data: &[u8]) -> rust_ipfs::IpfsPath {
    node.add_unixfs(data.to_vec())
        .chunk(Chunker::Size(64))
        .await
        .expect("add succeeds")
}

#[tokio::test]
async fn cat_roundtrip_multiblock() {
    let node = Node::new("cat_roundtrip_multiblock").await;
    let data = payload(40_000);

    let path = add_multiblock(&node, &data).await;
    let out = node.cat_unixfs(path).await.expect("cat succeeds");

    assert_eq!(out.len(), data.len());
    assert_eq!(out.as_ref(), data.as_slice());

    node.shutdown().await;
}

#[tokio::test]
async fn get_roundtrip_multiblock() {
    let node = Node::new("get_roundtrip_multiblock").await;
    let data = payload(40_000);

    let path = add_multiblock(&node, &data).await;

    let dest =
        std::env::temp_dir().join(format!("rust_ipfs_unixfs_get_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&dest);

    node.get_unixfs(path, &dest).await.expect("get succeeds");

    let written = std::fs::read(&dest).expect("read back");
    let _ = std::fs::remove_file(&dest);

    assert_eq!(written, data);

    node.shutdown().await;
}

#[tokio::test]
async fn cat_roundtrip_single_block() {
    let node = Node::new("cat_roundtrip_single_block").await;
    let data = payload(40);

    let path = add_multiblock(&node, &data).await;
    let out = node.cat_unixfs(path).await.expect("cat succeeds");

    assert_eq!(out.as_ref(), data.as_slice());

    node.shutdown().await;
}

#[tokio::test]
async fn ls_roundtrip_multiblock() {
    let node = Node::new("ls_roundtrip_multiblock").await;

    let path = add_multiblock(&node, &payload(40_000)).await;

    let entries = node.unixfs().ls(path).await.expect("ls walk succeeds");

    assert!(
        entries
            .iter()
            .any(|e| matches!(e, rust_ipfs::unixfs::Entry::File { .. })),
        "{entries:?}"
    );

    node.shutdown().await;
}

#[tokio::test]
async fn cat_roundtrip_default_chunk() {
    let node = Node::new("cat_roundtrip_default_chunk").await;
    // Larger than one default 256 KiB leaf so the standard chunker also spans multiple blocks.
    let data = payload(700_000);

    let path = node.add_unixfs(data.clone()).await.expect("add succeeds");
    let out = node.cat_unixfs(path).await.expect("cat succeeds");

    assert_eq!(out.as_ref(), data.as_slice());

    node.shutdown().await;
}
