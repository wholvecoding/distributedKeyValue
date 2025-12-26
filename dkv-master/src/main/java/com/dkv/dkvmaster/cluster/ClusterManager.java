 package com.dkv.dkvmaster.cluster;

import com.dkv.dkvmaster.router.ConsistentHashRouter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ClusterManager {
    private CuratorFramework client;
    private PathChildrenCache nodesCache;
    private ConsistentHashRouter router;

    public ClusterManager(ConsistentHashRouter router) {
        this.router = router;
    }

    public void start() throws Exception {
        // 1. 启动 ZK 客户端
        client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181") // ⚠️ 实际开发请放到配置文件里
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("dkv") // 所有的路径自动加上 /mini-dkv 前缀
                .build();
        client.start();

        // 2. 注册监听器，监听 /nodes 下的子节点变化
        // /dkv/nodes
        //      |-- 192.168.1.1:8080 (Child)
        //      |-- 192.168.1.2:8080 (Child)
        //true 表示把节点里的数据（Payload）缓存到本地
        nodesCache = new PathChildrenCache(client, "/nodes", true);
        nodesCache.start();

        nodesCache.getListenable().addListener((client, event) -> {
            // 情况 1：有新节点上线 (DataNode 启动了)
            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                // ZK 发来通知："/nodes/192.168.1.5:8080" 出现了
                String nodeIp = getNodeName(event.getData().getPath());

                // 考勤员大喊：哈希环，快把这个人加进去！
                router.addNode(nodeIp);
            }
            // 情况 2：有节点下线 (DataNode 挂了/断网了)
            else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                // ZK 发来通知："/nodes/192.168.1.5:8080" 消失了
                String nodeIp = getNodeName(event.getData().getPath());

                // 考勤员大喊：哈希环，把这个人踢出去！
                router.removeNode(nodeIp);
            }
        });

        System.out.println("Master 启动成功，正在监听节点变化...");
    }

    private String getNodeName(String fullPath) {
        // fullPath 可能是 /nodes/192.168.1.1:8080
        return fullPath.substring(fullPath.lastIndexOf("/") + 1);
    }
}
