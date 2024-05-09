use std::fmt::Debug;
use std::path::PathBuf;

//数据库启动时用户所进行的配置
#[derive(Clone)]
pub struct Options {
    //数据库目录
    pub dir_path: PathBuf,
    //数据文件大小(字节)
    pub data_file_size: u64,
    //由于page cache的存在,写文件会先写内存
    //每次写完文件是否持久化文件,如果这样做可以增加可靠性但是降低性能(可以优化为直接IO)
    pub sync_writes: bool,
    //目前只支持BTree
    pub index_type: IndexType,
}

#[derive(Clone, Copy)]
pub enum IndexType {
    BTree,
    SkipList,
}

//默认的选项
impl Default for Options {
    fn default() -> Self {
        Options {
            dir_path: std::env::temp_dir().join("bitcask"),
            data_file_size: 256 * 1024 * 1024, //256mb
            sync_writes: false,
            index_type: IndexType::BTree,
        }
    }
}

//索引迭代器配置项
#[derive(Clone)]
pub struct IteratorOptions {
    //prefix代表只找以prefix开头的key
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: vec![],
            reverse: false,
        }
    }
}

pub struct WriteBatchOptions {
    //一个批次中最大数据量,防止一次
    pub max_batch_num: usize,
    //提交的时候是否进行持久化
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 10000,
            sync_writes: true,
        }
    }
}
