package com.dkv.dkvmaster.router;

import com.dkv.dkvcommon.utils.HashUtil;

import java.util.*;
import java.util.stream.Stream;

/**
 * （一致性哈希）：
 * 当增加一个节点时，受影响的仅仅是环上该节点逆时针方向的那一小部分数据
 * 相比于取模哈希，显著减少了数据的移动
 */
public class ConsistentHashRouter {
    // 哈希环：Key是哈希值，Value是节点IP:Port
    private final SortedMap<Integer, String> ring = new TreeMap<>();

    // 虚拟节点数：每个物理节点在环上变成 10 个虚拟节点，解决数据倾斜问题
    private static final int VIRTUAL_NODES = 10;

    /**
     * 添加物理节点
     * @param nodeIp 例如 "192.168.1.5:8080"
     */
    public synchronized void addNode(String nodeIp) {
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            // 构造虚拟节点名称，例如 "192.168.1.5:8080#1"
            //作用：让数据分布更加均匀，避免数据倾斜
            String virtualNodeName = nodeIp + "#" + i;
            int hash = HashUtil.getHash(virtualNodeName);
            ring.put(hash, nodeIp);
        }
        System.out.println("节点上线: " + nodeIp + "，当前环大小: " + ring.size());
    }

    /**
     * 移除物理节点
     */
    public synchronized void removeNode(String nodeIp) {
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            String virtualNodeName = nodeIp + "#" + i;
            int hash = HashUtil.getHash(virtualNodeName);
            ring.remove(hash);
        }
        System.out.println("节点下线: " + nodeIp + "，当前环大小: " + ring.size());
    }

    /**
     * 路由算法：给一个数据Key，返回它该去哪个节点
     */
    public synchronized String routeNode(String key) {
        // 1. 算出数据 Key 的哈希值 (比如 hash = 1000)
        int hash = HashUtil.getHash(key);

        // 2. 核心：tailMap(hash) 方法
        // 它的含义是：从 TreeMap 中截取 "Key >= 1000" 的所有部分
        // 也就是在圆环上，从 1000 这个点开始，往顺时针方向找所有的节点
        SortedMap<Integer, String> tailMap = ring.tailMap(hash);

        // 3. 判断是否到了环的尽头
        Integer targetHash;
        if (tailMap.isEmpty()) {
            // 情况 A：tailMap 为空。
            // 说明 hash 值比环上所有节点的 hash 都大（比如 hash=9999，环上最大节点是 8000）。
            // 既然是圆环，走到尽头就要回到原点。
            // 所以我们取 ring.firstKey() —— 环上最小的那个节点。
            targetHash = ring.firstKey();
        } else {
            // 情况 B：tailMap 不为空。
            // 取 tailMap.firstKey()，也就是顺时针遇到的第一个节点。
            targetHash = tailMap.firstKey();
        }

        // 4. 返回那个节点的真实 IP
        return ring.get(targetHash);
    }
    /**
     * 返回一组节点（主节点 + 副本节点）
     * @param replicaCount 副本数量，通常为 3
     */
    public List<String> routeNodeWithReplicas(String key, int replicaCount) {
        if (ring.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> nodes = new ArrayList<>();
        int hash = HashUtil.getHash(key);

        // 1. 获取从该哈希值开始的所有后继节点
        SortedMap<Integer, String> tailMap = ring.tailMap(hash);

        // 2. 将后继节点和环的前半部分拼接（实现环形遍历）
        // distinct() 是为了跳过同一个物理节点的不同虚拟节点
        Iterator<String> it = Stream.concat(tailMap.values().stream(), ring.values().stream())
                .distinct()
                .iterator();

        while (it.hasNext() && nodes.size() < replicaCount) {
            nodes.add(it.next());
        }
        return nodes;
    }
}