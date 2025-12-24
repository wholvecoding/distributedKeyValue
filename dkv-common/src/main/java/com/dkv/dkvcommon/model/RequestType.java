package com.dkv.dkvcommon.model;

public enum RequestType {
    GET((byte) 1),
    PUT((byte) 2),
    DELETE((byte) 3);

    private byte code;

    RequestType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }
}