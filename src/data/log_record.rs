use prost::length_delimiter_len;

//logRecord写入到数据文件的记录.之所以叫日志,因为数据文件中数据是追加写入的,类似于日志的格式
#[derive(PartialEq)]
pub enum LogRecordType {
    //正常put的数据
    NORMAL = 1,
    //被删除的数据标识,墓碑值
    DELETE = 2,
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETE,
            _ => panic!("Unknown LogRecord type"),
        }
    }
}

pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }
    pub fn get_crc(&self) -> u32 {
        todo!()
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
    std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2
}
