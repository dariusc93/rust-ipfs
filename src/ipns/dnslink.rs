use crate::error::Error;
use crate::p2p::DnsResolver;
use crate::path::IpfsPath;

use tracing_futures::Instrument;

#[cfg(not(target_arch = "wasm32"))]
pub async fn resolve<'a>(
    resolver: DnsResolver,
    domain: &str,
    mut path: impl Iterator<Item = &'a str>,
) -> Result<IpfsPath, Error> {
    use hickory_resolver::AsyncResolver;
    use std::borrow::Cow;
    use std::str::FromStr;

    let span = tracing::trace_span!("dnslink", %domain);

    async move {
        // allow using non fqdn names (using the local search path suffices)
        let searched = Some(Cow::Borrowed(domain));

        let prefix = "_dnslink.";
        let prefixed = if !domain.starts_with(prefix) {
            let mut next = String::with_capacity(domain.len() + prefix.len());
            next.push_str(prefix);
            next.push_str(domain);
            Some(Cow::Owned(next))
        } else {
            None
        };

        let searched = searched.into_iter().chain(prefixed.into_iter());

        // FIXME: this uses caching trust-dns resolver even though it's discarded right away
        // when trust-dns support lands in future libp2p-dns investigate if we could share one, no need
        // to have multiple related caches.
        let (config, opt) = resolver.into();
        let resolver = AsyncResolver::tokio(config, opt);

        // previous implementation searched $domain and _dnslink.$domain concurrently. not sure did
        // `domain` assume fqdn names or not, but local suffices were not being searched on windows at
        // least. they are probably waste of time most of the time.
        for domain in searched {
            let res = match resolver.txt_lookup(&*domain).await {
                Ok(res) => res,
                Err(e) => {
                    tracing::debug!("resolving dnslink of {:?} failed: {}", domain, e);
                    continue;
                }
            };

            let mut paths =
                res.iter()
                    .flat_map(|txt| txt.iter())
                    .filter_map(|txt| {
                        if txt.starts_with(b"dnslink=") {
                            Some(&txt[b"dnslink=".len()..])
                        } else {
                            None
                        }
                    })
                    .map(|suffix| {
                        std::str::from_utf8(suffix)
                            .map_err(Error::from)
                            .and_then(IpfsPath::from_str)
                            .and_then(|mut internal_path| {
                                internal_path.path.push_split(path.by_ref()).map_err(|_| {
                                    crate::path::IpfsPathError::InvalidPath("".into())
                                })?;
                                Ok(internal_path)
                            })
                    });

            if let Some(Ok(x)) = paths.next() {
                tracing::trace!("dnslink found for {:?}", domain);
                return Ok(x);
            }

            tracing::trace!("zero TXT records found for {:?}", domain);
        }

        Err(anyhow::anyhow!("failed to resolve {:?}", domain))
    }
    .instrument(span)
    .await
}

#[cfg(target_arch = "wasm32")]
pub async fn resolve<'a>(
    _: DnsResolver,
    domain: &str,
    _: impl Iterator<Item = &'a str>,
) -> Result<IpfsPath, Error> {
    let span = tracing::trace_span!("dnslink", %domain);
    async move { anyhow::bail!("failed to resolve {domain}: unimplemented") }
        .instrument(span)
        .await
}

#[cfg(test)]
mod tests {
    use super::resolve;

    #[tokio::test]
    async fn resolve_ipfs_io() {
        tracing_subscriber::fmt::init();
        let res = resolve(
            crate::p2p::DnsResolver::Cloudflare,
            "ipfs.io",
            std::iter::empty(),
        )
        .await
        .unwrap()
        .to_string();
        assert_eq!(res, "/ipns/website.ipfs.io");
    }

    #[tokio::test]
    async fn resolve_website_ipfs_io() {
        let res = resolve(
            crate::p2p::DnsResolver::Cloudflare,
            "website.ipfs.io",
            std::iter::empty(),
        )
        .await
        .unwrap();

        assert!(
            matches!(res.root(), crate::path::PathRoot::Ipld(_)),
            "expected an /ipfs/cid path"
        );
    }
}
