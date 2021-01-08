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

    pub fn select<T: Selectable>(&self, field: &str) -> Result<Self> {
        T::select(self, field)
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

pub struct Slice<T> {
    data: Box<[u8]>,
    marker: PhantomData<T>,
}

impl<T: Archive> Slice<T> {
    pub fn decode(cid: &Cid, data: &[u8]) -> Result<Self> {
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
}

impl<T: Archive> Deref for Block<T> {
    type Target = Archived<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { archived_value::<T>(self.data.as_ref(), 0) }
    }
}

pub trait Selectable {
    fn select(cid: &Cid, field: &str) -> Result<Cid>;
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

    #[test]
    fn test_streaming_write() -> Result<()> {
        let mut data = AStruct::default();
        data.nested.number = 42;

        // encode the block and do some random access
        let block = Block::encode(&data, 80)?;
        assert_eq!(block.nested.number, 42);

        // construct a query
        let query = block
            .cid()
            .select::<AStruct>("nested")?
            .select::<BStruct>("number")?;
        // extract an authenticated byte slice
        let response = block.extract(query.start(), query.len())?;
        // check the byte slice
        let number = Slice::<u32>::decode(&query, &response)?;
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
        let query = cid
            .select::<AStruct>("nested")?
            .select::<BStruct>("number")?;
        let response = block.extract(query.start(), query.len())?;
        assert!(Slice::<u32>::decode(&query, &response).is_err());
        Ok(())
    }
}
