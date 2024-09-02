//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{fmt, ops::Deref, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gid([u8; 16]);

impl Gid {
    pub const NOT_DISCOVERED: Gid = Gid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
}

impl Default for Gid {
    fn default() -> Self {
        Gid::NOT_DISCOVERED
    }
}

impl Deref for Gid {
    type Target = [u8; 16];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<[u8; 16]> for Gid {
    fn from(key: [u8; 16]) -> Self {
        Self(key)
    }
}

impl From<&[u8; 16]> for Gid {
    fn from(key: &[u8; 16]) -> Self {
        Self(*key)
    }
}

impl Serialize for Gid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            // serialize as an hexadecimal String
            Serialize::serialize(&hex::encode(self.0), serializer)
        } else {
            // serialize as a little-endian [u8; 16]
            Serialize::serialize(&self.0, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Gid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            // deserialize from an hexadecimal String
            let s: &str = Deserialize::deserialize(deserializer)?;
            let v = hex::decode(s).map_err(|e| {
                serde::de::Error::custom(format!("Failed to decode gid {s} as hex: {e}"))
            })?;
            if v.len() == 16 {
                Ok(TryInto::<&[u8; 16]>::try_into(&v[..16]).unwrap().into())
            } else {
                Err(serde::de::Error::custom(format!(
                    "Failed to decode gid {s} as hex: not 16 bytes"
                )))
            }
        } else {
            // deserialize from a little-endian [u8; 16]
            let bytes: [u8; 16] = Deserialize::deserialize(deserializer)?;
            Ok(bytes.into())
        }
    }
}

impl fmt::Debug for Gid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self == &Gid::NOT_DISCOVERED {
            write!(f, "NOT_DISCOVERED")
        } else {
            let s = hex::encode(self.0);
            write!(f, "{s}")
        }
    }
}

impl fmt::Display for Gid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl FromStr for Gid {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0u8; 16];
        hex::decode_to_slice(s, &mut bytes).map_err(|e: hex::FromHexError| e.to_string())?;
        Ok(bytes.into())
    }
}

mod tests {
    #[test]
    fn test_gid() {
        use std::{ops::Deref, str::FromStr};

        use crate::gid::Gid;

        let str1 = "01106c8324a780d1b9e62c8f000001c1";
        let bytes1 = [
            0x01u8, 0x10, 0x6c, 0x83, 0x24, 0xa7, 0x80, 0xd1, 0xb9, 0xe6, 0x2c, 0x8f, 0x00, 0x00,
            0x01, 0xc1,
        ];

        assert_eq!(Gid::from(bytes1).deref(), &bytes1);
        assert_eq!(Gid::from(&bytes1).deref(), &bytes1);
        assert_eq!(Gid::from_str(str1).unwrap().deref(), &bytes1);
        assert_eq!(Gid::from(bytes1).to_string(), str1);
        assert_eq!(Gid::from(&bytes1).to_string(), str1);
        assert_eq!(Gid::from_str(str1).unwrap().to_string(), str1);

        let str2: &str = "01106c8324a780d1b9e62c8f00000e04";
        assert!(Gid::from_str(str2).unwrap() > Gid::from_str(str1).unwrap());

        assert!(Gid::from_str("01106c8324a780d1b9e62c8f00000e04aaaaaaaa").is_err());
    }

    #[test]
    fn test_serde() {
        use crate::gid::Gid;

        let bytes = [
            0x01u8, 0x10, 0x6c, 0x83, 0x24, 0xa7, 0x80, 0xd1, 0xb9, 0xe6, 0x2c, 0x8f, 0x00, 0x00,
            0x01, 0xc1,
        ];
        let json: &str = "\"01106c8324a780d1b9e62c8f000001c1\"";

        assert_eq!(serde_json::to_string(&Gid::from(&bytes)).unwrap(), json);
        assert_eq!(
            serde_json::from_str::<Gid>(json).unwrap(),
            Gid::from(&bytes)
        );

        // Check that endianness doesn't impact CDR serialization (note: 4 bytes CDR header ignored in comparison)
        assert_eq!(
            &cdr::serialize::<_, _, cdr::CdrBe>(&Gid::from(&bytes), cdr::Infinite).unwrap()[4..],
            &bytes
        );
        assert_eq!(
            &cdr::serialize::<_, _, cdr::CdrLe>(&Gid::from(&bytes), cdr::Infinite).unwrap()[4..],
            &bytes
        );
        let cdr = [
            0x0u8, 0, 1, 0, 0x01, 0x10, 0x6c, 0x83, 0x24, 0xa7, 0x80, 0xd1, 0xb9, 0xe6, 0x2c, 0x8f,
            0x00, 0x00, 0x01, 0xc1,
        ];
        assert_eq!(cdr::deserialize::<Gid>(&cdr).unwrap(), Gid::from(&bytes));
    }
}
