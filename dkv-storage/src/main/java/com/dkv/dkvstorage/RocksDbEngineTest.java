package com.dkv.dkvstorage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import com.dkv.dkvstorage.rocksdb.RocksDbEngine;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class RocksDbEngineTest {

    private RocksDbEngine storageEngine;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        storageEngine = new RocksDbEngine();
        storageEngine.init(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (storageEngine != null) {
            storageEngine.close();
        }
    }

    @Test
    @DisplayName("测试基本PUT和GET功能")
    void testBasicPutAndGet() throws Exception {
        // 测试正常写入和读取
        String key = "user:001";
        byte[] value = "张三".getBytes("UTF-8");

        storageEngine.put(key, value);

        byte[] result = storageEngine.get(key);
        assertNotNull(result);
        assertEquals("张三", new String(result, "UTF-8"));
    }

    @Test
    @DisplayName("测试GET不存在的key")
    void testGetNonExistentKey() throws Exception {
        // 查询不存在的key应该返回null
        byte[] result = storageEngine.get("non_existent_key");
        assertNull(result);
    }

    @Test
    @DisplayName("测试DELETE功能")
    void testDelete() throws Exception {
        // 先写入数据
        String key = "temp_key";
        storageEngine.put(key, "temp_value".getBytes());

        // 确认数据存在
        assertNotNull(storageEngine.get(key));

        // 删除数据
        storageEngine.delete(key);

        // 确认数据已被删除
        assertNull(storageEngine.get(key));
    }

    @Test
    @DisplayName("测试参数校验")
    void testParameterValidation() {
        // 测试空参数异常
        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.put(null, "value".getBytes());
        });

        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.put("key", null);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.get(null);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.delete(null);
        });
    }

    @Test
    @DisplayName("测试写入计数")
    void testWriteCount() throws Exception {
        // 初始计数应为0
        assertEquals(0, storageEngine.getWriteCount());

        // 写入3条数据
        storageEngine.put("key1", "value1".getBytes());
        storageEngine.put("key2", "value2".getBytes());
        storageEngine.put("key3", "value3".getBytes());

        // 验证计数
        assertEquals(3, storageEngine.getWriteCount());
    }

    @Test
    @DisplayName("测试覆盖写入")
    void testOverwrite() throws Exception {
        String key = "same_key";

        // 第一次写入
        storageEngine.put(key, "first_value".getBytes());
        assertEquals("first_value", new String(storageEngine.get(key)));

        // 覆盖写入
        storageEngine.put(key, "second_value".getBytes());
        assertEquals("second_value", new String(storageEngine.get(key)));

        // 写入计数应该增加2次
        assertEquals(2, storageEngine.getWriteCount());
    }

    @Test
    @DisplayName("测试特殊字符key")
    void testSpecialCharactersKey() throws Exception {
        // 测试包含特殊字符的key
        String[] specialKeys = {
                "key-with-dash",
                "key.with.dot",
                "key:with:colon",
                "key_with_underscore",
                "key with space",
                "key/with/slash",
                "key\\with\\backslash",
                "key@with@at",
                "key#with#hash",
                "key$with$dollar"
        };

        for (int i = 0; i < specialKeys.length; i++) {
            String key = specialKeys[i];
            byte[] value = ("value" + i).getBytes();

            storageEngine.put(key, value);

            byte[] result = storageEngine.get(key);
            assertNotNull(result);
            assertEquals("value" + i, new String(result));
        }
    }

    @Test
    @DisplayName("测试空value")
    void testEmptyValue() throws Exception {
        // 测试空字节数组
        storageEngine.put("empty_value_key", new byte[0]);

        byte[] result = storageEngine.get("empty_value_key");
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    @DisplayName("测试长key和value")
    void testLongKeyAndValue() throws Exception {
        // 生成长字符串
        StringBuilder longKey = new StringBuilder();
        StringBuilder longValue = new StringBuilder();
        Random random = new Random();

        for (int i = 0; i < 1000; i++) {
            longKey.append((char) ('a' + random.nextInt(26)));
            longValue.append((char) ('A' + random.nextInt(26)));
        }

        storageEngine.put(longKey.toString(), longValue.toString().getBytes());

        byte[] result = storageEngine.get(longKey.toString());
        assertNotNull(result);
        assertEquals(longValue.toString(), new String(result));
    }
}