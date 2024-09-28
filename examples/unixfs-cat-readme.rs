use std::str::FromStr;

use rust_ipfs::UninitializedIpfsDefault as UninitializedIpfs;
use rust_ipfs::{Ipfs, IpfsPath};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_default()
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .start()
        .await?;
    ipfs.default_bootstrap().await?;

    let readme_bytes = ipfs
        .cat_unixfs(IpfsPath::from_str(
            "/ipfs/QmS4ustL54uo8FzR9455qaxZwuMiUhyvMcX9Ba8nUH4uVv/readme",
        )?)
        .await?;

    let mut stdout = tokio::io::stdout();

    stdout.write_all(&readme_bytes).await?;

    Ok(())
}
