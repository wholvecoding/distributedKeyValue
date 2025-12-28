package com.dkv.dkvmaster.router;

import com.dkv.dkvcommon.utils.HashUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * （一致性哈希）：
 * 当增加一个节点时，受影响的仅仅是环上该节点逆时针方向的那一小部分数据
 * 相比于取模哈希，显著减少了数据的移动
 */
@Component
public class ConsistentHashRouter {
    // 哈希环：Key是哈希值，Value是节点IP:Port
    private final SortedMap<Integer, String> ring = new TreeMap<>();

    // 虚拟节点数：每个物理节点在环上变成 10 个虚拟节点，解决数据倾斜问题
    private static final int VIRTUAL_NODES = 10;

    /**
     * 添加物理节点
     * @param nodeIp 例如 "192.168.1.5:8080"
     */
    public synchronized void addNode(String nodeIp,Integer port ) {
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            // 构造虚拟节点名称，例如 "192.168.1.5:8080#1"
            //作用：让数据分布更加均匀，避免数据倾斜
            String virtualNodeName = nodeIp +":"+port+ "#" + i;
            int hash = HashUtil.getHash(virtualNodeName);
            ring.put(hash, nodeIp);
        }
        System.out.println("节点上线: " + nodeIp + "，当前环大小: " + ring.size());
    }

    /**
     * 移除物理节点
     */
    public synchronized void removeNode(String nodeIp, Integer port ) {
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            String virtualNodeName = nodeIp +":"+port+ "#" + i;
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
    public synchronized List<String> routeNodeWithReplicas(String key, int replicas) {
        if (ring.isEmpty()) {
            return new ArrayList<>();
        }

        // 使用 LinkedHashSet 保证顺序且去重（防止多个虚拟节点指向同一物理节点）
        Set<String> nodes = new LinkedHashSet<>();

        // 1. 算出数据 Key 的哈希值
        int hash = HashUtil.getHash(key);

        // 2. 获取顺时针方向的所有节点视图
        // tailMap 包含所有 hash >= keyHash 的节点
        SortedMap<Integer, String> tailMap = ring.tailMap(hash);

        // 3. 循环寻找足够的物理节点
        // 我们需要遍历两次环（或者使用一个迭代器循环），以防需要回滚到环首
        Iterator<String> tailIterator = tailMap.values().iterator();
        Iterator<String> headIterator = ring.values().iterator();

        // 先从当前位置往后找
        while (tailIterator.hasNext() && nodes.size() < replicas) {
            nodes.add(tailIterator.next());
        }

        // 如果还没找够，从环头（最小 hash）开始继续找（实现圆环逻辑）
        while (headIterator.hasNext() && nodes.size() < replicas) {
            nodes.add(headIterator.next());
        }

        return new ArrayList<>(nodes);
    }
}