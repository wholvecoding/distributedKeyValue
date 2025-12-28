package com.dkv.dkvmaster.cluster;

import com.dkv.dkvmaster.router.ConsistentHashRouter;
import jakarta.annotation.PostConstruct;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class ClusterManager {
    private CuratorFramework client;
    private PathChildrenCache nodesCache;
    private ConsistentHashRouter router;

    public ClusterManager(ConsistentHashRouter router) {
        this.router = router;
    }
    @PostConstruct
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
            String nodeIPport = getNodeName(event.getData().getPath());
            String nodeIp = nodeIPport.split(":")[0];
            Integer port = Integer.valueOf(nodeIPport.split(":")[1]);

            // 情况 1：有新节点上线 (DataNode 启动了)
            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                // ZK 发来通知："/nodes/192.168.1.5:8080" 出现了

                router.addNode(nodeIp,port );
            }
            // 情况 2：有节点下线 (DataNode 挂了/断网了)
            else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                // ZK 发来通知："/nodes/192.168.1.5:8080" 消失了
               nodeIp = getNodeName(event.getData().getPath());

                router.removeNode(nodeIp,port);
            }
        });

        System.out.println("Master 启动成功，正在监听节点变化...");
    }

    // 在 ClusterManager.java 中添加或修改
    public void addNodeToZk(String nodeIp) throws Exception {
        String path = "/nodes/" + nodeIp;
        // 检查节点是否存在，不存在则创建
        if (client.checkExists().forPath(path) == null) {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL) // 临时节点，断开就消失
                    .forPath(path);
        }
    }
    public void offlineNode(String nodeIp, Integer port ) throws Exception {
        // 1. 从 ZooKeeper 中删除对应的临时节点
        // 假设路径格式为 /nodes/127.0.0.1:9001
        String path = "/nodes/" + nodeIp+":"+port;
        if (client.checkExists().forPath(path) != null) {
            client.delete().forPath(path);
            System.out.println("ZooKeeper 节点已强制移除: " + path);
        }

        // 2. 调用 router 清理本地哈希环
        router.removeNode(nodeIp,port);
    }
    /**
     * 获取当前所有在线的 DataNode 列表
     * @return 节点 IP:Port 字符串列表
     */
    public List<String> getOnlineNodes() {
        // 1. nodesCache.getCurrentData() 会返回当前监听到的所有子节点数据列表
        List<ChildData> currentData = nodesCache.getCurrentData();

        // 2. 使用 Java 8 Stream 流进行转换
        return currentData.stream()
                // 获取每个节点的完整路径，例如 "/nodes/127.0.0.1:8080"
                .map(ChildData::getPath)
                // 调用你之前写好的 getNodeName 方法，把路径切分，只留 IP:Port
                .map(this::getNodeName)
                // 转换为 List 返回
                .collect(Collectors.toList());
    }
    private String getNodeName(String fullPath) {
        // fullPath 可能是 /nodes/192.168.1.1:8080
        return fullPath.substring(fullPath.lastIndexOf("/") + 1);
    }
}
