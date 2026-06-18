use futures::StreamExt;
use rust_ipfs::builder::DefaultIpfsBuilder;
use rust_ipfs::mfs::{MfsKind, WriteOptions};
use rust_ipfs::unixfs::UnixfsStatus;
use rust_ipfs::Ipfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let ipfs: Ipfs = DefaultIpfsBuilder::new().start().await?;

    let mfs = ipfs.mfs();

    println!("> mkdir");
    mfs.mkdir("/docs/notes", true).await?;
    mfs.mkdir("/media", false).await?;

    println!("> write files");
    mfs.write("/docs/readme.txt", b"hello mfs", true).await?;
    mfs.write("/docs/notes/todo.txt", b"buy milk\nwrite docs\n", true)
        .await?;

    println!("> write from a stream");
    let chunks = vec![
        Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"streamed ")),
        Ok(bytes::Bytes::from_static(b"line by ")),
        Ok(bytes::Bytes::from_static(b"line\n")),
    ];
    mfs.write_stream("/docs/streamed.txt", futures::stream::iter(chunks), false)
        .await?;
    println!(
        "  {}",
        String::from_utf8_lossy(&mfs.read("/docs/streamed.txt").await?)
    );

    println!("> ls /");
    print_ls(&mfs, "/").await?;
    println!("> ls /docs");
    print_ls(&mfs, "/docs").await?;

    println!("> read /docs/readme.txt");
    println!(
        "  {}",
        String::from_utf8_lossy(&mfs.read("/docs/readme.txt").await?)
    );

    println!("> read /docs/streamed.txt as a stream");
    let stream = mfs.read_stream("/docs/streamed.txt");
    futures::pin_mut!(stream);
    while let Some(chunk) = stream.next().await {
        print!("  chunk: {}", String::from_utf8_lossy(&chunk?));
    }

    println!("> stat /docs/notes/todo.txt");
    let st = mfs.stat("/docs/notes/todo.txt").await?;
    println!("  cid={} size={} {:?}", st.cid, st.size, st.kind);

    println!("> random-access write (offset 6)");
    mfs.write_with(
        "/docs/readme.txt",
        b"MFS!!",
        WriteOptions {
            offset: 6,
            ..Default::default()
        },
    )
    .await?;
    println!(
        "  {}",
        String::from_utf8_lossy(&mfs.read("/docs/readme.txt").await?)
    );

    println!("> append past EOF");
    let len = mfs.stat("/docs/readme.txt").await?.size;
    mfs.write_with(
        "/docs/readme.txt",
        b" + appended",
        WriteOptions {
            offset: len,
            ..Default::default()
        },
    )
    .await?;
    println!(
        "  {}",
        String::from_utf8_lossy(&mfs.read("/docs/readme.txt").await?)
    );

    println!("> truncate to 9 bytes");
    mfs.truncate("/docs/readme.txt", 9).await?;
    println!(
        "  {}",
        String::from_utf8_lossy(&mfs.read("/docs/readme.txt").await?)
    );

    println!("> cp within mfs");
    mfs.cp("/docs/readme.txt", "/media/copy.txt", false).await?;
    println!(
        "  {}",
        String::from_utf8_lossy(&mfs.read("/media/copy.txt").await?)
    );

    println!("> cp from /ipfs (import external content)");
    let imported = add_to_ipfs(
        &ipfs,
        "greeting.txt",
        b"added through ipfs and imported into mfs",
    )
    .await?;
    mfs.cp(&imported, "/media/imported.txt", false).await?;
    println!(
        "  {}",
        String::from_utf8_lossy(&mfs.read("/media/imported.txt").await?)
    );

    println!("> mv");
    mfs.mv("/media/copy.txt", "/media/renamed.txt", false)
        .await?;
    print_ls(&mfs, "/media").await?;

    println!("> rm -r /docs");
    mfs.rm("/docs", true).await?;
    print_ls(&mfs, "/").await?;

    println!("> root cid");
    println!("  {:?}", mfs.root().await?);

    ipfs.exit_daemon().await;
    Ok(())
}

async fn print_ls(mfs: &rust_ipfs::mfs::Mfs, path: &str) -> anyhow::Result<()> {
    for entry in mfs.ls(path).await? {
        let kind = match entry.kind {
            MfsKind::File { size } => format!("file, {size} bytes"),
            MfsKind::Directory => "dir".to_string(),
            MfsKind::Symlink => "symlink".to_string(),
        };
        println!("  {:<16} [{}]  {}", entry.name, kind, entry.cid);
    }
    Ok(())
}

async fn add_to_ipfs(ipfs: &Ipfs, name: &str, data: &[u8]) -> anyhow::Result<String> {
    let data = data.to_vec();
    let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(data) }).boxed();
    let mut add = ipfs.unixfs().add((name.to_string(), stream));

    let mut path = None;
    while let Some(status) = add.next().await {
        match status {
            UnixfsStatus::CompletedStatus { path: p, .. } => path = Some(p),
            UnixfsStatus::FailedStatus { error, .. } => anyhow::bail!(error),
            UnixfsStatus::ProgressStatus { .. } => {}
        }
    }
    Ok(path.expect("add completed").to_string())
}
