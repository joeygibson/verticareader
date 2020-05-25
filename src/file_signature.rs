use core::fmt;
use std::error;
use std::error::Error;
use std::fmt::Formatter;
use std::fs::File;

use crate::read_u8;

const FILE_SIGNATURE_LENGTH: usize = 11;
const VALID_FILE_SIGNATURE_BYTES: [u8; 11] = [
    0x4e, 0x41, 0x54, 0x49, 0x56, 0x45, 0x0a, 0xff, 0x0d, 0x0a, 0x00,
];

#[derive(Debug)]
pub struct FileSignature {
    data: [u8; 11],
}

#[derive(Debug, Clone)]
struct FileSignatureError;

impl fmt::Display for FileSignatureError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "header is invalid")
    }
}

impl error::Error for FileSignatureError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl FileSignature {
    pub fn from_reader(reader: &mut File) -> Result<Self, Box<dyn Error>> {
        let mut data: [u8; 11] = [0; 11];

        for i in 0..FILE_SIGNATURE_LENGTH {
            let byte = read_u8(reader)?;
            data[i] = byte;
        }

        validate(&data)?;

        Ok(FileSignature { data })
    }
}

fn validate(data: &[u8; 11]) -> Result<(), Box<dyn Error>> {
    for (expected, value) in VALID_FILE_SIGNATURE_BYTES.iter().zip(data.iter()) {
        if expected != value {
            return Err(Box::new(FileSignatureError));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::file_signature::FileSignature;

    #[test]
    fn test_read_from_good_file() {
        let mut file = File::open("data/all-types.bin").unwrap();

        let res = FileSignature::from_reader(&mut file);

        assert!(res.is_ok())
    }

    #[test]
    fn test_read_from_bad_file() {
        let mut file = File::open("data/all-types-with-bad-signature.bin").unwrap();

        let res = FileSignature::from_reader(&mut file);

        assert!(res.is_err())
    }
}
