use std::{borrow::Cow, str};

use crate::{Deserializable, Serializable};

impl Serializable for String {
    const IS_FIXED_SIZE: bool = false;
    fn serialize(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
    fn serialize_fixed_size(&self) -> Option<[u8; 8]> {
        None
    }
}

impl Deserializable for String {
    fn from_bytes(bytes: &[u8]) -> Self {
        str::from_utf8(bytes).unwrap().to_string()
    }
}

impl Serializable for str {
    const IS_FIXED_SIZE: bool = false;
    fn serialize(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
    fn serialize_fixed_size(&self) -> Option<[u8; 8]> {
        None
    }
}

impl Serializable for Vec<u8> {
    const IS_FIXED_SIZE: bool = false;
    fn serialize(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self)
    }
    fn serialize_fixed_size(&self) -> Option<[u8; 8]> {
        None
    }
}

impl Deserializable for Vec<u8> {
    fn from_bytes(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}

macro_rules! implement_integer_key_type {
    ($integer_type:ident) => {
        impl Deserializable for $integer_type {
            fn from_bytes(bytes: &[u8]) -> Self {
                let mut buf = [0; std::mem::size_of::<$integer_type>()];
                buf.copy_from_slice(&bytes[..std::mem::size_of::<$integer_type>()]);
                $integer_type::from_le_bytes(buf)
            }
        }

        impl Serializable for $integer_type {
            const IS_FIXED_SIZE: bool = true;
            fn serialize(&self) -> Cow<'_, [u8]> {
                Cow::Owned(self.to_le_bytes().to_vec())
            }
            fn serialize_fixed_size(&self) -> Option<[u8; 8]> {
                let mut buf = [0; 8];
                buf[..std::mem::size_of::<$integer_type>()]
                    .copy_from_slice(&self.to_le_bytes()[..]);
                Some(buf)
            }
        }
    };
}

implement_integer_key_type!(u64);
implement_integer_key_type!(i64);
implement_integer_key_type!(u32);
implement_integer_key_type!(i32);
implement_integer_key_type!(u16);
implement_integer_key_type!(i16);
implement_integer_key_type!(u8);
implement_integer_key_type!(i8);
