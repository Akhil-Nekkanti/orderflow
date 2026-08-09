//! # wire — fixed-layout binary order-entry protocol
//!
//! A compact little-endian wire format in the spirit of exchange
//! order-entry protocols (OUCH, native binary gateways):
//!
//! - every message is a 1-byte type tag followed by fixed-width fields;
//! - message sizes are known from the tag alone, so there is no length
//!   prefix and framing is trivial;
//! - decoding reads straight out of the input slice with `from_le_bytes`
//!   (safe, alignment-free, and compiles to plain loads) — no allocation,
//!   no copies of the payload buffer.
//!
//! Layouts (offsets after the tag byte):
//!
//! | tag | message | fields | size |
//! |-----|---------|--------|------|
//! | 1 | `New`     | id u64, price i64, qty u64, side u8, tif u8 | 27 |
//! | 2 | `Cancel`  | id u64 | 9 |
//! | 3 | `Replace` | id u64, price i64, qty u64 | 25 |
//! | 4 | `Market`  | id u64, qty u64, side u8 | 18 |

use lob::{OrderId, Price, Qty, Side, Tif};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Msg {
    New {
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: Tif,
    },
    Cancel {
        id: OrderId,
    },
    Replace {
        id: OrderId,
        price: Price,
        qty: Qty,
    },
    Market {
        id: OrderId,
        side: Side,
        qty: Qty,
    },
}

const TAG_NEW: u8 = 1;
const TAG_CANCEL: u8 = 2;
const TAG_REPLACE: u8 = 3;
const TAG_MARKET: u8 = 4;

const SIZE_NEW: usize = 1 + 8 + 8 + 8 + 1 + 1;
const SIZE_CANCEL: usize = 1 + 8;
const SIZE_REPLACE: usize = 1 + 8 + 8 + 8;
const SIZE_MARKET: usize = 1 + 8 + 8 + 1;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DecodeError {
    /// First byte is not a known message tag.
    BadTag(u8),
    /// Side byte is not 0 (bid) or 1 (ask).
    BadSide(u8),
    /// Tif byte is not 0 (GTC), 1 (IOC), or 2 (FOK).
    BadTif(u8),
    /// Buffer ends mid-message.
    Truncated,
}

#[inline]
fn side_byte(s: Side) -> u8 {
    match s {
        Side::Bid => 0,
        Side::Ask => 1,
    }
}

#[inline]
fn tif_byte(t: Tif) -> u8 {
    match t {
        Tif::Gtc => 0,
        Tif::Ioc => 1,
        Tif::Fok => 2,
    }
}

#[inline]
fn parse_side(b: u8) -> Result<Side, DecodeError> {
    match b {
        0 => Ok(Side::Bid),
        1 => Ok(Side::Ask),
        b => Err(DecodeError::BadSide(b)),
    }
}

#[inline]
fn parse_tif(b: u8) -> Result<Tif, DecodeError> {
    match b {
        0 => Ok(Tif::Gtc),
        1 => Ok(Tif::Ioc),
        2 => Ok(Tif::Fok),
        b => Err(DecodeError::BadTif(b)),
    }
}

/// Appends the encoded form of `msg` to `out`.
pub fn encode(msg: &Msg, out: &mut Vec<u8>) {
    match *msg {
        Msg::New {
            id,
            side,
            price,
            qty,
            tif,
        } => {
            out.push(TAG_NEW);
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
            out.push(side_byte(side));
            out.push(tif_byte(tif));
        }
        Msg::Cancel { id } => {
            out.push(TAG_CANCEL);
            out.extend_from_slice(&id.to_le_bytes());
        }
        Msg::Replace { id, price, qty } => {
            out.push(TAG_REPLACE);
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
        }
        Msg::Market { id, side, qty } => {
            out.push(TAG_MARKET);
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
            out.push(side_byte(side));
        }
    }
}

#[inline]
fn read_u64(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(buf[at..at + 8].try_into().unwrap())
}

#[inline]
fn read_i64(buf: &[u8], at: usize) -> i64 {
    i64::from_le_bytes(buf[at..at + 8].try_into().unwrap())
}

/// Decodes one message from the front of `buf`. Returns the message and the
/// number of bytes consumed.
pub fn decode(buf: &[u8]) -> Result<(Msg, usize), DecodeError> {
    let &tag = buf.first().ok_or(DecodeError::Truncated)?;
    match tag {
        TAG_NEW => {
            if buf.len() < SIZE_NEW {
                return Err(DecodeError::Truncated);
            }
            let msg = Msg::New {
                id: read_u64(buf, 1),
                price: read_i64(buf, 9),
                qty: read_u64(buf, 17),
                side: parse_side(buf[25])?,
                tif: parse_tif(buf[26])?,
            };
            Ok((msg, SIZE_NEW))
        }
        TAG_CANCEL => {
            if buf.len() < SIZE_CANCEL {
                return Err(DecodeError::Truncated);
            }
            Ok((
                Msg::Cancel {
                    id: read_u64(buf, 1),
                },
                SIZE_CANCEL,
            ))
        }
        TAG_REPLACE => {
            if buf.len() < SIZE_REPLACE {
                return Err(DecodeError::Truncated);
            }
            let msg = Msg::Replace {
                id: read_u64(buf, 1),
                price: read_i64(buf, 9),
                qty: read_u64(buf, 17),
            };
            Ok((msg, SIZE_REPLACE))
        }
        TAG_MARKET => {
            if buf.len() < SIZE_MARKET {
                return Err(DecodeError::Truncated);
            }
            let msg = Msg::Market {
                id: read_u64(buf, 1),
                qty: read_u64(buf, 9),
                side: parse_side(buf[17])?,
            };
            Ok((msg, SIZE_MARKET))
        }
        tag => Err(DecodeError::BadTag(tag)),
    }
}

/// Iterator over a buffer of back-to-back messages.
pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len()
    }
}

impl Iterator for Reader<'_> {
    type Item = Result<Msg, DecodeError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }
        match decode(self.buf) {
            Ok((msg, used)) => {
                self.buf = &self.buf[used..];
                Some(Ok(msg))
            }
            Err(e) => {
                self.buf = &[]; // poison: stop after first error
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: Msg) {
        let mut buf = Vec::new();
        encode(&msg, &mut buf);
        let (back, used) = decode(&buf).expect("decode");
        assert_eq!(back, msg);
        assert_eq!(used, buf.len());
    }

    #[test]
    fn roundtrips_all_variants() {
        roundtrip(Msg::New {
            id: u64::MAX,
            side: Side::Bid,
            price: -5,
            qty: 123,
            tif: Tif::Fok,
        });
        roundtrip(Msg::Cancel { id: 7 });
        roundtrip(Msg::Replace {
            id: 9,
            price: 10_000,
            qty: 1,
        });
        roundtrip(Msg::Market {
            id: 11,
            side: Side::Ask,
            qty: 42,
        });
    }

    #[test]
    fn reader_streams_multiple_messages() {
        let msgs = [
            Msg::New {
                id: 1,
                side: Side::Ask,
                price: 100,
                qty: 5,
                tif: Tif::Gtc,
            },
            Msg::Cancel { id: 1 },
            Msg::Market {
                id: 2,
                side: Side::Bid,
                qty: 3,
            },
        ];
        let mut buf = Vec::new();
        for m in &msgs {
            encode(m, &mut buf);
        }
        let decoded: Vec<Msg> = Reader::new(&buf).map(|r| r.unwrap()).collect();
        assert_eq!(decoded, msgs);
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(decode(&[0xFF]), Err(DecodeError::BadTag(0xFF)));
        assert_eq!(decode(&[TAG_NEW, 0, 0]), Err(DecodeError::Truncated));
        let mut buf = Vec::new();
        encode(
            &Msg::New {
                id: 1,
                side: Side::Bid,
                price: 1,
                qty: 1,
                tif: Tif::Gtc,
            },
            &mut buf,
        );
        buf[25] = 9; // corrupt side byte
        assert_eq!(decode(&buf), Err(DecodeError::BadSide(9)));
    }
}
