use std::str::FromStr;

use futures::StreamExt;
use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;
use rust_ipfs::{Ipfs, IpfsPath};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ipfs: Ipfs = UninitializedIpfs::new().with_default().start().await?;
    ipfs.default_bootstrap().await?;

    let mut stream = ipfs
        .cat_unixfs(
            IpfsPath::from_str("/ipfs/QmS4ustL54uo8FzR9455qaxZwuMiUhyvMcX9Ba8nUH4uVv/readme")?,
            None,
        )
        .await?
        .boxed();

    let mut stdout = tokio::io::stdout();

    while let Some(result) = stream.next().await {
        match result {
            Ok(bytes) => {
                stdout.write_all(&bytes).await?;
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    Ok(())
}
