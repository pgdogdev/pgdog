use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
pub struct ParameterDescription {
    params: Vec<i32>,
}

impl FromBytes for ParameterDescription {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 't');
        let _len = bytes.get_i32();
        let num_params = bytes.get_u16();
        let mut params = Vec::with_capacity(num_params as usize);
        for _ in 0..num_params as usize {
            params.push(bytes.get_i32());
        }
        Ok(Self { params })
    }
}

impl ToBytes for ParameterDescription {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_u16(self.params.len() as u16);
        for param in &self.params {
            payload.put_i32(*param);
        }

        Ok(payload.freeze())
    }
}

impl Protocol for ParameterDescription {
    fn code(&self) -> char {
        't'
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parameter_description_round_trip_small() {
        let params = vec![23, 42, 87];
        let description = ParameterDescription { params: params.clone() };

        let bytes = description.to_bytes().unwrap();
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u8(), b't');
        let len = buf.get_i32();
        assert_eq!(len as usize, bytes.len() - 1); // message length excludes the leading code
        assert_eq!(buf.get_u16(), params.len() as u16);

        let decoded = ParameterDescription::from_bytes(bytes).unwrap();
        assert_eq!(decoded.params, params);
    }

    #[test]
    fn parameter_description_round_trip_max_parameter_count() {
        let count = u16::MAX as usize;
        let params: Vec<i32> = (0..count).map(|i| i as i32).collect();
        let description = ParameterDescription { params: params.clone() };

        let bytes = description.to_bytes().unwrap();
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u8(), b't');
        let len = buf.get_i32();
        assert_eq!(len as usize, bytes.len() - 1);
        assert_eq!(buf.get_u16(), count as u16);

        let decoded = ParameterDescription::from_bytes(bytes).unwrap();
        assert_eq!(decoded.params.len(), count);
        assert_eq!(decoded.params[0], 0);
        assert_eq!(decoded.params[count - 1], (count - 1) as i32);
    }
}
