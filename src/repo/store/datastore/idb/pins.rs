use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use crate::repo::{BlockStore, PinModeRequirement};
use crate::{Error, PinKind, PinMode};
use idb::ObjectStore;
use ipld_core::cid::Cid;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use wasm_bindgen_futures::wasm_bindgen::JsValue;

const VERSION: u8 = 1;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RecursiveRecord {
    version: u8,
    references: BTreeSet<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IndirectRecord {
    version: u8,
    roots: BTreeSet<String>,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub(super) struct PinRecords {
    pub(super) direct: BTreeSet<Cid>,
    pub(super) recursive: BTreeMap<Cid, Option<BTreeSet<Cid>>>,
    pub(super) indirect: BTreeMap<Cid, BTreeSet<Cid>>,
    pub(super) legacy: bool,
}

impl PinRecords {
    pub(super) fn owners(&self) -> Result<BTreeMap<Cid, BTreeSet<Cid>>, Error> {
        let mut owners: BTreeMap<Cid, BTreeSet<Cid>> = BTreeMap::new();
        for (root, references) in &self.recursive {
            let references = references
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("pin ownership has not been rebuilt for {root}"))?;
            for cid in references {
                owners.entry(*cid).or_default().insert(*root);
            }
        }
        Ok(owners)
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.legacy {
            anyhow::bail!("pin ownership has not been rebuilt");
        }
        if self.indirect != self.owners()? {
            anyhow::bail!("pin ownership does not match recursive references");
        }
        Ok(())
    }

    pub(super) fn list(&self, requirement: Option<PinMode>) -> Vec<(Cid, PinMode)> {
        let requirement = PinModeRequirement::from(requirement);
        let mut returned = BTreeSet::new();
        let mut pins = Vec::new();
        let mut add = |cid: &Cid, mode| {
            if requirement.matches(&mode) && returned.insert(*cid) {
                pins.push((*cid, mode));
            }
        };
        for cid in self.recursive.keys() {
            add(cid, PinMode::Recursive);
        }
        for cid in &self.direct {
            add(cid, PinMode::Direct);
        }
        for cid in self.indirect.keys() {
            add(cid, PinMode::Indirect);
        }
        pins
    }
}

#[derive(Default)]
pub(super) struct Pin {
    pub(super) direct: bool,
    pub(super) recursive: Option<BTreeSet<Cid>>,
    pub(super) indirect: BTreeSet<Cid>,
}

impl Pin {
    pub(super) fn kind(&self, requirement: Option<PinMode>) -> Option<PinKind<Cid>> {
        let requirement = PinModeRequirement::from(requirement);
        if let Some(references) = &self.recursive
            && requirement.matches(&PinMode::Recursive)
        {
            return Some(PinKind::Recursive(references.len() as u64));
        }
        if self.direct && requirement.matches(&PinMode::Direct) {
            return Some(PinKind::Direct);
        }
        if requirement.matches(&PinMode::Indirect) {
            return self.indirect.first().copied().map(PinKind::IndirectFrom);
        }
        None
    }
}

fn pin_key(cid: &Cid, mode: PinMode) -> String {
    let mode = match mode {
        PinMode::Direct => "d",
        PinMode::Recursive => "r",
        PinMode::Indirect => "i",
    };
    format!("pin.{mode}.{cid}")
}

fn parse_pin_key(key: &str) -> Result<(Cid, PinMode), Error> {
    let (mode, cid) = key
        .strip_prefix("pin.")
        .and_then(|suffix| suffix.split_once('.'))
        .ok_or_else(|| anyhow::anyhow!("invalid pin key {key}"))?;
    let mode = match mode {
        "d" => PinMode::Direct,
        "r" => PinMode::Recursive,
        "i" => PinMode::Indirect,
        _ => anyhow::bail!("invalid pin mode in key {key}"),
    };
    let cid = Cid::from_str(cid)
        .map_err(|error| Error::from(error).context(format!("invalid CID in pin key {key}")))?;
    if pin_key(&cid, mode) != key {
        anyhow::bail!("invalid pin key {key}");
    }
    Ok((cid, mode))
}

fn decode<T: DeserializeOwned>(value: JsValue) -> Result<T, Error> {
    serde_wasm_bindgen::from_value(value).map_err(|error| anyhow::anyhow!("{error}"))
}

fn parse_cids(values: BTreeSet<String>) -> Result<BTreeSet<Cid>, Error> {
    values
        .into_iter()
        .map(|value| {
            Cid::from_str(&value)
                .map_err(|error| Error::from(error).context(format!("invalid pin CID {value}")))
        })
        .collect()
}

fn validate_direct(value: JsValue) -> Result<(), Error> {
    let bytes: Vec<u8> = decode(value)?;
    if !bytes.is_empty() {
        anyhow::bail!("invalid direct pin value");
    }
    Ok(())
}

fn recursive_references(
    value: JsValue,
    allow_legacy: bool,
) -> Result<Option<BTreeSet<Cid>>, Error> {
    if allow_legacy
        && serde_wasm_bindgen::from_value::<Vec<u8>>(value.clone())
            .is_ok_and(|bytes| bytes.is_empty())
    {
        return Ok(None);
    }
    let record: RecursiveRecord = decode(value)?;
    if record.version != VERSION {
        anyhow::bail!("unsupported recursive pin version {}", record.version);
    }
    Ok(Some(parse_cids(record.references)?))
}

fn indirect_roots(value: JsValue, allow_legacy: bool) -> Result<(BTreeSet<Cid>, bool), Error> {
    if allow_legacy && let Some(root) = value.as_string() {
        return Ok((BTreeSet::from([Cid::from_str(&root)?]), true));
    }
    let record: IndirectRecord = decode(value)?;
    if record.version != VERSION {
        anyhow::bail!("unsupported indirect pin version {}", record.version);
    }
    if record.roots.is_empty() {
        anyhow::bail!("indirect pin has no owners");
    }
    Ok((parse_cids(record.roots)?, false))
}

pub(super) async fn read_pin_records(
    store: &ObjectStore,
    allow_legacy: bool,
) -> Result<PinRecords, Box<dyn std::error::Error>> {
    let key_request = store.get_all_keys(None, None)?;
    let value_request = store.get_all(None, None)?;
    let keys = key_request.await?;
    let values = value_request.await?;
    if keys.len() != values.len() {
        return Err(std::io::Error::other("pin keys and values do not match").into());
    }

    let mut pins = PinRecords::default();
    for (key, value) in keys.into_iter().zip(values) {
        let key = key
            .as_string()
            .ok_or_else(|| std::io::Error::other("invalid pin key"))?;
        let (cid, mode) = parse_pin_key(&key)?;
        match mode {
            PinMode::Direct => {
                validate_direct(value)
                    .map_err(|error| error.context(format!("invalid direct pin for {cid}")))?;
                pins.direct.insert(cid);
            }
            PinMode::Recursive => {
                let references = recursive_references(value, allow_legacy)
                    .map_err(|error| error.context(format!("invalid recursive pin for {cid}")))?;
                if references.as_ref().is_some_and(|refs| refs.contains(&cid)) {
                    return Err(anyhow::anyhow!("recursive pin refers to itself for {cid}").into());
                }
                pins.legacy |= references.is_none();
                pins.recursive.insert(cid, references);
            }
            PinMode::Indirect => {
                let (roots, legacy) = indirect_roots(value, allow_legacy)
                    .map_err(|error| error.context(format!("invalid indirect pin for {cid}")))?;
                pins.legacy |= legacy;
                pins.indirect.insert(cid, roots);
            }
        }
    }
    Ok(pins)
}

pub(super) async fn read_pin(
    store: &ObjectStore,
    cid: &Cid,
) -> Result<Pin, Box<dyn std::error::Error>> {
    let recursive = store.get(JsValue::from_str(&pin_key(cid, PinMode::Recursive)))?;
    let direct = store.get(JsValue::from_str(&pin_key(cid, PinMode::Direct)))?;
    let indirect = store.get(JsValue::from_str(&pin_key(cid, PinMode::Indirect)))?;
    let mut pin = Pin::default();
    if let Some(value) = recursive.await? {
        pin.recursive = recursive_references(value, false)
            .map_err(|error| error.context(format!("invalid recursive pin for {cid}")))?;
        if pin
            .recursive
            .as_ref()
            .is_some_and(|refs| refs.contains(cid))
        {
            return Err(anyhow::anyhow!("recursive pin refers to itself for {cid}").into());
        }
    }
    if let Some(value) = direct.await? {
        validate_direct(value)
            .map_err(|error| error.context(format!("invalid direct pin for {cid}")))?;
        pin.direct = true;
    }
    if let Some(value) = indirect.await? {
        pin.indirect = indirect_roots(value, false)
            .map_err(|error| error.context(format!("invalid indirect pin for {cid}")))?
            .0;
    }
    Ok(pin)
}

pub(super) async fn read_indirect(
    store: &ObjectStore,
    cid: &Cid,
) -> Result<BTreeSet<Cid>, Box<dyn std::error::Error>> {
    match store
        .get(JsValue::from_str(&pin_key(cid, PinMode::Indirect)))?
        .await?
    {
        Some(value) => Ok(indirect_roots(value, false)
            .map_err(|error| error.context(format!("invalid indirect pin for {cid}")))?
            .0),
        None => Ok(BTreeSet::new()),
    }
}

pub(super) async fn delete_pin(
    store: &ObjectStore,
    cid: &Cid,
    mode: PinMode,
) -> Result<(), Box<dyn std::error::Error>> {
    store
        .delete(JsValue::from_str(&pin_key(cid, mode)))?
        .await?;
    Ok(())
}

pub(super) async fn write_direct(
    store: &ObjectStore,
    cid: &Cid,
) -> Result<(), Box<dyn std::error::Error>> {
    let value = serde_wasm_bindgen::to_value(&Vec::<u8>::new())?;
    store
        .put(
            &value,
            Some(&JsValue::from_str(&pin_key(cid, PinMode::Direct))),
        )?
        .await?;
    Ok(())
}

pub(super) async fn write_recursive(
    store: &ObjectStore,
    cid: &Cid,
    references: &BTreeSet<Cid>,
) -> Result<(), Box<dyn std::error::Error>> {
    let record = RecursiveRecord {
        version: VERSION,
        references: references.iter().map(Cid::to_string).collect(),
    };
    let value = serde_wasm_bindgen::to_value(&record)?;
    store
        .put(
            &value,
            Some(&JsValue::from_str(&pin_key(cid, PinMode::Recursive))),
        )?
        .await?;
    Ok(())
}

pub(super) async fn write_indirect(
    store: &ObjectStore,
    cid: &Cid,
    roots: &BTreeSet<Cid>,
) -> Result<(), Box<dyn std::error::Error>> {
    if roots.is_empty() {
        return delete_pin(store, cid, PinMode::Indirect).await;
    }
    let record = IndirectRecord {
        version: VERSION,
        roots: roots.iter().map(Cid::to_string).collect(),
    };
    let value = serde_wasm_bindgen::to_value(&record)?;
    store
        .put(
            &value,
            Some(&JsValue::from_str(&pin_key(cid, PinMode::Indirect))),
        )?
        .await?;
    Ok(())
}

pub(super) async fn write_pin_records(
    store: &ObjectStore,
    pins: &PinRecords,
) -> Result<(), Box<dyn std::error::Error>> {
    pins.validate()?;
    store.clear()?.await?;
    for cid in &pins.direct {
        write_direct(store, cid).await?;
    }
    for (root, references) in &pins.recursive {
        let references = references
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("pin ownership has not been rebuilt for {root}"))?;
        write_recursive(store, root, references).await?;
    }
    for (cid, roots) in &pins.indirect {
        write_indirect(store, cid, roots).await?;
    }
    Ok(())
}

pub(super) async fn collect_block_references(
    block_store: &impl BlockStore,
    root: Cid,
) -> Result<BTreeSet<Cid>, Error> {
    let mut visited = BTreeSet::from([root]);
    let mut pending = vec![root];
    while let Some(cid) = pending.pop() {
        let block = block_store
            .get(&cid)
            .await
            .map_err(|error| error.context(format!("failed to read pinned block {cid}")))?
            .ok_or_else(|| anyhow::anyhow!("missing pinned block {cid}"))?;
        let mut references = BTreeSet::new();
        block
            .references(&mut references)
            .map_err(|error| Error::from(error).context(format!("invalid pinned block {cid}")))?;
        for reference in references {
            if visited.insert(reference) {
                pending.push(reference);
            }
        }
    }
    visited.remove(&root);
    Ok(visited)
}
