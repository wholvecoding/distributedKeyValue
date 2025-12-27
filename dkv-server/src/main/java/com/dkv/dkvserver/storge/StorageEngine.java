package com.dkv.dkvserver.storge;

public interface StorageEngine {
    void init(String dbPath);
    void put(String key, byte[] value);
    byte[] get(String key);
    void delete(String key);
}
