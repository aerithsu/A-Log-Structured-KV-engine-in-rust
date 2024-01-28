use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

//logRecord写入到数据文件的记录.之所以叫日志,因为数据文件中数据是追加写入的,类似于日志的格式
#[derive(PartialEq, Copy, Clone, Debug)]
pub enum LogRecordType {
    //正常put的数据
    NORMAL = 1,
    //被删除的数据标识,墓碑值
    DELETED = 2,
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            _ => panic!("Unknown LogRecord type"),
        }
    }
}

#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    // encode 对 LogRecord 进行编码，返回字节数组及长度
    //
    //	+-------------+--------------+-------------+--------------+-------------+-------------+
    //	|  type 类型   |    key size |   value size |      key    |      value   |  crc 校验值  |
    //	+-------------+-------------+--------------+--------------+-------------+-------------+
    //	    1字节        变长（最大5）   变长（最大5）        变长           变长           4字节
    pub fn encode(&self) -> Vec<u8> {
        //存放编码数据的字节数组
        self.encode_and_get_crc().0
    }
    pub fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        //存放编码数据的字节数组
        let mut buf = BytesMut::new();
        buf.reserve(self.encode_length());

        //第一个字节存放Type
        buf.put_u8(self.rec_type as u8);

        //借助prost库存储key和value的长度
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        //计算出crc校验值
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);
        (buf.to_vec(), crc)
    }
    pub fn get_crc(&self) -> u32 {
        self.encode_and_get_crc().1
    }
    //计算log_record编码后的长度
    fn encode_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

//数据位置索引信息，描述数据存储到了什么位置
#[derive(Debug, Copy, Clone)]
pub struct LogRecordPos {
    //pub(crate)保证只在crate里为public的
    pub(crate) file_id: u32,
    //文件id
    pub(crate) offset: u64, //文件偏移
}

pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}

//获取log_record header部分的最大长度
#[inline]
pub fn max_log_record_header_size() -> usize {
    //length_delimiter对于不同大小usize值编码后的长度不同
    std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode_and_crc() {
        // 正常的一条 LogRecord 编码
        let rec1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc1 = rec1.encode();
        assert!(enc1.len() > 5);
        assert_eq!(1020360578, rec1.get_crc());

        // LogRecord 的 value 为空
        let rec2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc2 = rec2.encode();
        assert!(enc2.len() > 5);
        assert_eq!(3756865478, rec2.get_crc());

        // 类型为 Deleted 的情况
        let rec3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::DELETED,
        };
        let enc3 = rec3.encode();
        assert!(enc3.len() > 5);
        assert_eq!(1867197446, rec3.get_crc());
    }
}
