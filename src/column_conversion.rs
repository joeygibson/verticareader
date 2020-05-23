use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[derive(Debug)]
pub enum ColumnConversion {
    IpAddress,
    MacAddress,
}

impl ColumnConversion {
    pub fn from_string(string: &str) -> Result<ColumnConversion, String> {
        let result = match string.to_lowercase().as_str() {
            "ipaddress" => ColumnConversion::IpAddress,
            "macaddress" => ColumnConversion::MacAddress,
            _ => return Err(format!("invalid conversion: {}", string.clone())),
        };

        Ok(result)
    }

    pub fn convert(&self, bytes: Vec<u8>) -> String {
        match &*self {
            ColumnConversion::IpAddress => {
                if bytes[0] == 0xff && bytes[1] == 0xff {
                    let tmp: Vec<String> =
                        bytes[2..].iter().map(|b| format!("{:0>2X}", b)).collect();
                    let addr = u32::from_str_radix(&tmp.join(""), 16).map(Ipv4Addr::from);

                    match addr {
                        Err(e) => {
                            eprintln!("error: {}", e);
                            "".to_string()
                        }
                        Ok(s) => s.to_string(),
                    }
                } else {
                    let tmp_bytes = if bytes.len() < 16 {
                        let mut tmp_bytes: Vec<u8> = vec![0; 16];

                        for (index, b) in bytes.iter().enumerate() {
                            tmp_bytes[index] = *b;
                        }

                        tmp_bytes
                    } else {
                        bytes
                    };

                    let tmp: Vec<String> =
                        tmp_bytes.iter().map(|b| format!("{:0>2X}", b)).collect();
                    match u128::from_str_radix(&tmp.join(""), 16) {
                        Err(e) => {
                            eprintln!("error: {}", e);
                            "".to_string()
                        }
                        Ok(s) => {
                            let addr: IpAddr = Ipv6Addr::from(s).into();
                            addr.to_string()
                        }
                    }
                }
            }
            ColumnConversion::MacAddress => {
                let addr: Vec<String> = bytes.iter().map(|b| format!("{:0>2X}", b)).collect();
                addr.join(":")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::column_conversion::ColumnConversion;

    #[test]
    fn test_ip_v4() {
        let bytes = vec![0xFFu8, 0xFFu8, 0xC0u8, 0xA8u8, 0xBu8, 0x2u8];

        let cnv = ColumnConversion::IpAddress;
        let val = cnv.convert(bytes);

        assert_eq!("192.168.11.2", val);
    }

    #[test]
    fn test_ip_v6() {
        let bytes = vec![
            0x20u8, 0x1u8, 0x4u8, 0x2u8, 0x4u8, 0x23u8, 0xFFu8, 0xFEu8, 0x9Eu8, 0xF1u8, 0x6Eu8,
        ];

        let cnv = ColumnConversion::IpAddress;
        let val = cnv.convert(bytes);

        assert_eq!("2001:402:423:fffe:9ef1:6e00::", val);
    }

    #[test]
    fn test_mac() {
        let bytes = vec![0xF4u8, 0xF, 0x1B, 0x28, 0xF2, 0x4C];

        let cnv = ColumnConversion::MacAddress;
        let val = cnv.convert(bytes);

        assert_eq!("F4:0F:1B:28:F2:4C", val);
    }
}
