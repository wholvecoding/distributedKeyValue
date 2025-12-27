package com.dkv.dkvserver.storge;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;

public class RocksDbEngine implements StorageEngine {

    static {
        try {
            // 自动加载 Windows 下的 DLL
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("win")) {
                RocksDB.loadLibrary(); // 会自动从 jar 中提取 librocksdbjni-win64.dll
            } else {
                RocksDB.loadLibrary();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load RocksDB native library", e);
        }
    }

    private RocksDB db;
    private BloomFilter<CharSequence> bloomFilter;

    static {
        RocksDB.loadLibrary();
    }

    @Override
    public void init(String dbPath) {
        try {
            Options options = new Options()
                    .setCreateIfMissing(true);

            db = RocksDB.open(options, dbPath);

            // 初始化布隆过滤器（10 万 key，1% 误判率）
            bloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    100_000,
                    0.01
            );
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to init RocksDB", e);
        }
    }

    @Override
    public void put(String key, byte[] value) {
        try {
            bloomFilter.put(key);
            db.put(key.getBytes(), value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] get(String key) {
        if (!bloomFilter.mightContain(key)) {
            return null;
        }
        try {
            return db.get(key.getBytes());
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void delete(String key) {
        try {
            db.delete(key.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
