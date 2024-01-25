use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

//logRecord写入到数据文件的记录.之所以叫日志,因为数据文件中数据是追加写入的,类似于日志的格式
#[derive(PartialEq, Copy, Clone)]
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
	std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2
}
