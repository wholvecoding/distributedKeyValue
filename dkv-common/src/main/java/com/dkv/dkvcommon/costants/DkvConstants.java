package com.dkv.dkvcommon.costants;

public class DkvConstants {
    // ZooKeeper 根路径
    public static final String ZK_ROOT_PATH = "/mini-dkv";

    // 节点注册路径 (DataNode 启动时在这里创建临时节点)
    // 结构: /mini-dkv/nodes/192.168.1.5:8080
    public static final String ZK_NODES_PATH = "/mini-dkv/nodes";

    // 默认 DataNode 服务端口
    public static final int DEFAULT_SERVER_PORT = 8080;
}