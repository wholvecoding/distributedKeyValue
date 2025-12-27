package kvr;

import java.io.Serializable;

/**
 * RPC 响应对象
 * Server -> Client
 */
public class KvResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    // 状态码定义
    public static final int OK = 200;
    public static final int NOT_FOUND = 404;
    public static final int ERROR = 500;

    /**
     * 响应状态码
     */
    private int code;

    /**
     * 返回的数据（GET 时使用）
     */
    private byte[] value;

    /**
     * 描述信息（错误信息 / 成功提示）
     */
    private String message;

    // ================= 构造方法 =================

    public KvResponse() {
    }

    public KvResponse(int code, byte[] value, String message) {
        this.code = code;
        this.value = value;
        this.message = message;
    }

    // 快捷工厂方法（推荐使用）
    public static KvResponse ok(byte[] value) {
        return new KvResponse(OK, value, "OK");
    }

    public static KvResponse notFound(String msg) {
        return new KvResponse(NOT_FOUND, null, msg);
    }

    public static KvResponse error(String msg) {
        return new KvResponse(ERROR, null, msg);
    }

    // ================= Getter / Setter =================

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    // ================= 调试输出 =================

    @Override
    public String toString() {
        return "KvResponse{" +
                "code=" + code +
                ", valueLength=" + (value == null ? 0 : value.length) +
                ", message='" + message + '\'' +
                '}';
    }
}
