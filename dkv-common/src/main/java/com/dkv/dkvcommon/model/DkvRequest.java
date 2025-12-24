package com.dkv.dkvcommon.model;

import lombok.Data;
import java.io.Serializable;

@Data
public class DkvRequest implements Serializable {
    // 请求类型：GET/PUT/DELETE
    private RequestType type;

    // 键
    private String key;

    // 值 (PUT时使用，GET/DELETE 为空)
    // 使用 byte[] 而不是 String，是为了支持图片、文件等二进制存储
    private byte[] value;

    // 请求ID (用于异步匹配响应，类似快递单号)
    private String requestId;
}