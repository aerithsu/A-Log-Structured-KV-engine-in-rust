use bytes::Bytes;

pub fn get_test_key(i: i32) -> Bytes {
    Bytes::from(format!("bitcask-key-{:09}", i))
}

pub fn get_test_value(i: i32) -> Bytes {
    Bytes::from(format!("bitcask-value-{:09}", i))
}

#[test]
fn test_get_test_key() {
    assert_eq!(
        Bytes::from(format!("bitcask-key-{:09}", 1)),
        get_test_key(1)
    );
    assert_eq!(
        Bytes::from(format!("bitcask-value-{:09}", 3)),
        get_test_value(3)
    );
}
