use anyhow::Result;
use memoffset::span_of;
use random_access_block::{Block, Cid, Query};
use rkyv::{Archive, Archived};

#[derive(Archive, Default, PartialEq)]
struct AStruct {
    boolean: bool,
    nested: BStruct,
    link: Cid,
    text: String,
}

impl AStruct {
    fn select_nested(query: &Query<Self>) -> Query<BStruct> {
        query.select(span_of!(Archived<Self>, nested))
    }
}

#[derive(Archive, Default, PartialEq)]
struct BStruct {
    prefix: bool,
    number: u32,
}

impl BStruct {
    fn select_number(query: &Query<Self>) -> Query<u32> {
        query.select(span_of!(Archived<Self>, number))
    }
}

fn main() -> Result<()> {
    let mut data = AStruct::default();
    data.nested.number = 42;

    // encode the block and do some random access
    let block = Block::encode(&data, 80)?;
    assert_eq!(block.nested.number, 42);

    // construct a query
    let query = BStruct::select_number(&AStruct::select_nested(&Query::new(*block.cid())));
    // extract an authenticated byte slice
    let response = block.extract(query.start(), query.len())?;
    // check the byte slice
    let number = query.decode(&response)?;
    assert_eq!(*number, 42);
    Ok(())
}
