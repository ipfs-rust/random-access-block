use anyhow::Result;
use memoffset::span_of;
use random_access_block::{Block, Cid, Selectable, Slice};
use rkyv::{Archive, Archived};

#[derive(Archive, Default, PartialEq)]
struct AStruct {
    boolean: bool,
    nested: BStruct,
    link: Cid,
    text: String,
}

impl Selectable for AStruct {
    fn select(cid: &Cid, field: &str) -> Result<Cid> {
        Ok(match field {
            "boolean" => cid.slice(span_of!(Archived<Self>, boolean)),
            "nested" => cid.slice(span_of!(Archived<Self>, nested)),
            "link" => cid.slice(span_of!(Archived<Self>, link)),
            "text" => cid.slice(span_of!(Archived<Self>, text)),
            _ => anyhow::bail!("invalid key"),
        })
    }
}

#[derive(Archive, Default, PartialEq)]
struct BStruct {
    prefix: bool,
    number: u32,
}

impl Selectable for BStruct {
    fn select(cid: &Cid, field: &str) -> Result<Cid> {
        Ok(match field {
            "prefix" => cid.slice(span_of!(Archived<Self>, prefix)),
            "number" => cid.slice(span_of!(Archived<Self>, number)),
            _ => anyhow::bail!("invalid key"),
        })
    }
}

fn main() -> Result<()> {
    let mut data = AStruct::default();
    data.nested.number = 42;

    // encode the block and do some random access
    let block = Block::encode(&data, 80)?;
    assert_eq!(block.nested.number, 42);

    // construct a query
    let cid = block
        .cid()
        .select::<AStruct>("nested")?
        .select::<BStruct>("number")?;
    // extract an authenticated byte slice
    let response = block.extract(cid.start(), cid.len())?;
    // check the byte slice
    let number = Slice::<u32>::decode(&cid, &response)?;
    assert_eq!(*number, 42);
    Ok(())
}
