package kvr;

import java.io.Serializable;

/**
 * RPC 请求对象
 * 用于 Client -> Server 的通信
 */
public class KvRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    // 操作类型定义
    public static final byte PUT = 1;
    public static final byte GET = 2;
    public static final byte DELETE = 3;

    /**
     * 请求类型：PUT / GET / DELETE
     */
    private byte type;

    /**
     * 键
     */
    private String key;

    /**
     * 值（GET 请求时可以为 null）
     */
    private byte[] value;

    /**
     * 时间戳（可用于冲突解决、版本控制）
     */
    private long timestamp;

    // ================= 构造方法 =================

    public KvRequest() {
    }

    public KvRequest(byte type, String key, byte[] value) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }

    // ================= Getter / Setter =================

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // ================= 调试输出 =================

    @Override
    public String toString() {
        return "KvRequest{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", valueLength=" + (value == null ? 0 : value.length) +
                ", timestamp=" + timestamp +
                '}';
    }
}
