package com.dkv.dkvcommon.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class DkvResponse implements Serializable {
    // 状态码：200=成功, 404=Key不存在, 500=系统错误
    private int code;

    // 错误信息 (如果 code!=200)
    private String message;

    // 返回的数据 (GET请求会有值)
    private byte[] data;

    // 对应的请求ID
    private String requestId;
}