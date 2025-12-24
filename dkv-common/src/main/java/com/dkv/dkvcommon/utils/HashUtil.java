package com.dkv.dkvcommon.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public class HashUtil {
    // 使用 MurmurHash3，速度快且冲突少，适合分布式哈希
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32_fixed();

    /**
     * 计算字符串的哈希值 (非负数)
     */
    public static int getHash(String key) {
        if(key == null) {
            return 0;
        }
        int hash = HASH_FUNCTION.hashString(key, StandardCharsets.UTF_8).asInt();
        // 保证是正数 (取绝对值)
        return hash < 0 ? Math.abs(hash) : hash;
    }
}