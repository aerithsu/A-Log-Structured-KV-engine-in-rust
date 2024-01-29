use bitcask::db;
use bitcask::options::Options;
use bytes::Bytes;

fn main() {
    let engine = db::Engine::open(Options::default()).expect("failed to open bitcask engine");
    assert!(engine
        .put(Bytes::from("name"), Bytes::from("bitcask"))
        .is_ok());
    let res = engine.get(Bytes::from("name"));
    assert!(res.is_ok());
    let val = res.unwrap();
    assert_eq!(val, Bytes::from("bitcask"));
    let res = engine.delete(Bytes::from("name"));
    assert!(res.is_ok());
}
