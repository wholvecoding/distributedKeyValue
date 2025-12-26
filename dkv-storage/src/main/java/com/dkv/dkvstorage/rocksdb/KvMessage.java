// KvMessage.java
package com.dkv.dkvstorage.rocksdb;
import java.io.Serializable;
import java.util.Arrays;

public class KvMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Type {
        PUT(1),
        GET(2),
        DELETE(3),
        REPLICATION_PUT(4),  // 复制专用
        RESPONSE(5);

        private final int value;

        Type(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static Type fromValue(int value) {
            for (Type type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            return null;
        }
    }

    private Type type;
    private String key;
    private byte[] value;
    private int statusCode;
    private String message;
    private String requestId;
    private long timestamp;
    private boolean isReplication;

    // 构造方法
    public KvMessage(Type type, String key, byte[] value) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }

    // Getters and Setters
    public Type getType() { return type; }
    public void setType(Type type) { this.type = type; }

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }

    public byte[] getValue() { return value; }
    public void setValue(byte[] value) { this.value = value; }

    public int getStatusCode() { return statusCode; }
    public void setStatusCode(int statusCode) { this.statusCode = statusCode; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public boolean isReplication() { return isReplication; }
    public void setReplication(boolean replication) { isReplication = replication; }

    @Override
    public String toString() {
        return "KvMessage{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", valueSize=" + (value != null ? value.length : 0) +
                ", statusCode=" + statusCode +
                ", isReplication=" + isReplication +
                '}';
    }
}