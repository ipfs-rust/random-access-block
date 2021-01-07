use anyhow::Result;
use bao::decode::SliceDecoder;
use bao::encode::SliceExtractor;
use bao::Hash;
use rkyv::{archived_value, Archive, ArchiveBuffer, Archived, Write, WriteExt};
use std::convert::TryInto;
use std::io::{Cursor, Read};
use std::marker::PhantomData;
use std::ops::{Deref, Range};

pub const EMPTY_BLOCK_HASH: [u8; 32] = [
    175, 19, 73, 185, 245, 249, 161, 166, 160, 64, 77, 234, 54, 220, 201, 73, 155, 203, 37, 201,
    173, 193, 18, 183, 204, 154, 147, 202, 228, 31, 50, 98,
];

#[derive(Archive, Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Cid {
    version: u8,
    hash: [u8; 32],
    start: u64,
    len: u64,
}

impl Cid {
    pub fn new(hash: [u8; 32], len: usize) -> Self {
        Self {
            version: 0,
            hash,
            start: 0,
            len: len as _,
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            version: self.version,
            hash: self.hash,
            start: self.start + range.start as u64,
            len: (range.end - range.start) as _,
        }
    }

    pub fn hash(&self) -> Hash {
        Hash::from(self.hash)
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn len(&self) -> u64 {
        self.len
    }
}

impl std::fmt::Display for Cid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}[{}..{}]",
            self.hash().to_hex(),
            self.start(),
            self.start() + self.len()
        )
    }
}

impl Default for Cid {
    fn default() -> Self {
        Self {
            version: 0,
            hash: EMPTY_BLOCK_HASH,
            start: 0,
            len: 0,
        }
    }
}

pub struct Query<T: Archive> {
    cid: Cid,
    marker: PhantomData<T>,
}

impl<T: Archive> Query<T> {
    pub fn new(cid: Cid) -> Self {
        Self {
            cid,
            marker: PhantomData,
        }
    }

    pub fn select<T2: Archive>(&self, range: Range<usize>) -> Query<T2> {
        Query {
            cid: self.cid.slice(range),
            marker: PhantomData,
        }
    }

    pub fn decode(&self, response: &[u8]) -> Result<Slice<T>> {
        Slice::decode(self.cid, response)
    }
}

impl<T: Archive> std::ops::Deref for Query<T> {
    type Target = Cid;

    fn deref(&self) -> &Self::Target {
        &self.cid
    }
}

pub struct Slice<T> {
    data: Box<[u8]>,
    marker: PhantomData<T>,
}

impl<T: Archive> Slice<T> {
    fn decode(cid: Cid, data: &[u8]) -> Result<Self> {
        let mut buf = Vec::with_capacity(cid.len().try_into()?);
        let mut decoder = SliceDecoder::new(data, &cid.hash(), cid.start(), cid.len());
        decoder.read_to_end(&mut buf)?;
        // TODO return error
        assert_eq!(buf.len(), cid.len() as usize);
        Ok(Self {
            data: buf.into_boxed_slice(),
            marker: PhantomData,
        })
    }
}

impl<T: Archive> Deref for Slice<T> {
    type Target = Archived<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { archived_value::<T>(self.data.as_ref(), 0) }
    }
}

#[derive(Clone, Debug)]
pub struct Block<T: Archive> {
    data: Box<[u8]>,
    marker: PhantomData<T>,

    outboard: Box<[u8]>,
    cid: Cid,
}

impl<T: Archive> Block<T> {
    pub fn new(data: Box<[u8]>) -> Self {
        let (outboard, hash) = bao::encode::outboard(&data);
        let cid = Cid::new(*hash.as_bytes(), data.len());
        Self {
            data,
            marker: PhantomData,

            outboard: outboard.into_boxed_slice(),
            cid,
        }
    }

    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    pub fn encode(value: &T, max_buf_size: usize) -> Result<Self> {
        //let size = bao::encode::encoded_size(value.max_encoded_size());
        //let mut buf = Vec::with_capacity(size.try_into()?);
        let mut buf = vec![0; max_buf_size];
        let mut encoder = ArchiveBuffer::new(&mut buf);
        encoder.archive(value).unwrap();
        let len = encoder.pos();
        buf.resize(len, 0);

        Ok(Self::new(buf.into_boxed_slice()))
    }

    pub fn extract(&self, start: u64, len: u64) -> Result<Box<[u8]>> {
        let input = Cursor::new(&self.data);
        let outboard = Cursor::new(&self.outboard);
        let mut extractor = SliceExtractor::new_outboard(input, outboard, start, len);
        let size = bao::encode::encoded_size(len).try_into()?;
        let mut buf = Vec::with_capacity(size);
        extractor.read_to_end(&mut buf)?;
        Ok(buf.into_boxed_slice())
    }

    pub fn query(&self) -> Query<T> {
        Query {
            cid: self.cid,
            marker: self.marker,
        }
    }
}

impl<T: Archive> Deref for Block<T> {
    type Target = Archived<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { archived_value::<T>(self.data.as_ref(), 0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memoffset::span_of;

    #[test]
    fn hash_empty_block() {
        assert_eq!(blake3::hash(b"").as_bytes(), &EMPTY_BLOCK_HASH);
    }

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

    #[test]
    fn test_streaming_write() -> Result<()> {
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

    #[test]
    fn test_authentication() -> Result<()> {
        let mut data = AStruct::default();
        data.nested.number = 42;
        let block = Block::encode(&data, 80)?;
        let cid = *block.cid();

        data.nested.number = 43;
        let block = Block::encode(&data, 80)?;
        let query = BStruct::select_number(&AStruct::select_nested(&Query::new(cid)));
        let response = block.extract(query.start(), query.len())?;
        assert!(query.decode(&response).is_err());
        Ok(())
    }
}
