use std::path::PathBuf;

//数据库启动时用户所进行的配置
#[derive(Clone)]
pub struct Options {
    //数据库目录
    pub dir_path: PathBuf,
    //数据文件大小
    pub data_file_size: u64,
    //每次写完文件是否持久化文件,如果这样做可以增加可靠性但是降低性能
    pub sync_writes: bool,
    pub index_type: IndexType,
}

#[derive(Clone, Copy)]
pub enum IndexType {
    BTree,
    SkipList,
}
