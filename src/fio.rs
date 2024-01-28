use std::path::PathBuf;

use crate::errors::Result;
use crate::fio::file_io::FileIO;

pub mod file_io;

pub trait IOManager: Sync + Send {
    //从文件的制定位置读取相应的数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    //写入字节数组到文件中
    fn write(&self, buf: &[u8]) -> Result<usize>;
    //sync持久化数据
    fn sync(&self) -> Result<()>;
}

//根据文件名称初始化IOManager,目前只实现了文件IO
pub fn new_io_manager(file_name: PathBuf) -> Result<Box<dyn IOManager>> {
    let file_io = FileIO::new(&file_name);
    match file_io {
        Ok(file_io) => Ok(Box::new(file_io)),
        Err(e) => Err(e),
    }
}
