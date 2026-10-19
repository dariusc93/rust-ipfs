use std::io;
use std::path::Path;

use tempfile::{Builder, NamedTempFile};
use tokio::fs::File;

pub(crate) fn create(destination: &Path) -> io::Result<(NamedTempFile, File)> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut builder = Builder::new();
    builder.prefix(".rust-ipfs-");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o666));
    }

    let temporary = builder.tempfile_in(parent)?;
    if let Ok(metadata) = std::fs::metadata(destination) {
        temporary
            .as_file()
            .set_permissions(metadata.permissions())?;
    }
    let file = File::from_std(temporary.reopen()?);
    Ok((temporary, file))
}

pub(crate) fn persist(temporary: NamedTempFile, destination: &Path) -> io::Result<()> {
    temporary
        .persist(destination)
        .map_err(|error| error.error)?;

    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if let Ok(directory) = std::fs::File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}
