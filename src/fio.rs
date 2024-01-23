use crate::errors::Result;
pub mod file_io;

pub trait IOManager: Sync + Send {
    //从文件的制定位置读取相应的数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    //写入字节数组到文件中
    fn write(&self, buf: &[u8]) -> Result<usize>;
    //sync持久化数据
    fn sync(&self) -> Result<()>;
}
