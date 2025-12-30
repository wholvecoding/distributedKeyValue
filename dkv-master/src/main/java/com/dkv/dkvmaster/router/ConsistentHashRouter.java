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

    public Set<String> getRings() {
        return new HashSet<>(ring.values());
    }
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
            ring.put(hash, nodeIp +":"+port);
        }
//        System.out.println("节点上线: " + nodeIp + "，当前环大小: " + ring.size());
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
//        System.out.println("节点下线: " + nodeIp + "，当前环大小: " + ring.size());
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
        while (tailIterator.hasNext() && nodes.size() <=replicas) {
            nodes.add(tailIterator.next());
        }

        // 如果还没找够，从环头（最小 hash）开始继续找（实现圆环逻辑）
        while (headIterator.hasNext() && nodes.size() < replicas) {
            nodes.add(headIterator.next());
        }

        return new ArrayList<>(nodes);
    }
    // 在 ConsistentHashRouter.java 中添加
    public synchronized String getPredecessorNode(int hash) {
        SortedMap<Integer, String> headMap = ring.headMap(hash);
        if (headMap.isEmpty()) {
            return ring.get(ring.lastKey()); // 环状逻辑：取最后一个节点
        }
        return ring.get(headMap.lastKey());
    }

    public synchronized String getSuccessorNode(int hash) {
        SortedMap<Integer, String> tailMap = ring.tailMap(hash + 1); // 找严格大于当前hash的
        if (tailMap.isEmpty()) {
            return ring.get(ring.firstKey()); // 环状逻辑：取第一个节点
        }
        return ring.get(tailMap.firstKey());
    }
//    public synchronized void addNodeWithBalancedMigration(String nodeIp, Integer port) {
//        List<Integer> affectedHashes = new ArrayList<>();
//        // 计算新节点的虚拟节点哈希值
//        for (int i = 0; i < VIRTUAL_NODES; i++) {
//            String virtualNodeName = nodeIp + ":" + port + "#" + i;
//            int hash = HashUtil.getHash(virtualNodeName);
//            affectedHashes.add(hash);
//        }
//
//        // 计算新节点相邻的前后节点，并迁移数据
//        for (int i = 0; i < VIRTUAL_NODES; i++) {
//            String virtualNodeName = nodeIp + ":" + port + "#" + i;
//            int hash = HashUtil.getHash(virtualNodeName);
//
//            // 获取新节点相邻的前驱节点和后继节点
//            String predecessor = getPredecessorNode(hash);
//            String successor = getSuccessorNode(hash);
//
//            // 仅迁移受影响的部分数据
//            migrateData(predecessor, successor, nodeIp, port);
//        }
//
//        // 执行节点添加
//        for (int i = 0; i < VIRTUAL_NODES; i++) {
//            String virtualNodeName = nodeIp + ":" + port + "#" + i;
//            int hash = HashUtil.getHash(virtualNodeName);
//            ring.put(hash, nodeIp + ":" + port);
//        }
//    }

    // 数据迁移方法：根据前后节点获取受影响的数据并迁移
//    private void migrateData(String predecessor, String successor, String nodeIp, Integer port) {
//        List<String> affectedKeys = getAffectedKeys(predecessor, successor);
//
//        for (String key : affectedKeys) {
//            String currentNode = routeNode(key);
//            if (!currentNode.equals(nodeIp + ":" + port)) {
//                // 迁移数据到新节点
//                migrateKeyToNewNode(key, nodeIp + ":" + port);
//            }
//        }
//    }

    // 获取受影响的键：假设从前驱节点到后继节点之间的键都需要迁移
//    private List<String> getAffectedKeys(String predecessor, String successor) {
//        // 这里的实现依赖于具体的数据存储系统，我们假设RocksDB支持按范围查询
//        List<String> keys = new ArrayList<>();
//        try {
//            // 假设你有一个方法来查询指定范围内的所有键
//            ReadOptions readOptions = new ReadOptions();
//            RocksIterator iterator = rocksDB.newIterator(readOptions);
//
//            // 从前驱节点的哈希值开始，直到后继节点的哈希值
//            // 假设 getHash 生成的哈希值与键的顺序相关，实际中需要依据你的键的设计来调整
//            byte[] startKey = predecessor.getBytes();
//            byte[] endKey = successor.getBytes();
//
//            iterator.seek(startKey);
//            while (iterator.isValid() && Arrays.compare(iterator.key(), endKey) < 0) {
//                keys.add(new String(iterator.key()));
//                iterator.next();
//            }
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        }
//        return keys;
//    }
//
//    // 迁移数据到新节点
//    private void migrateKeyToNewNode(String key, String newNode) {
//        try {
//            // 从原节点读取数据并存储到新节点
//            byte[] value = rocksDB.get(key.getBytes());
//
//            // 将数据迁移到新的节点（假设有方法支持这种操作）
//            // 这里只是示例，具体的操作可以依据你的存储设计
//            // 保存数据到新节点，可以选择将数据写入新节点相关的区域
//            rocksDB.put(key.getBytes(), value);
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        }
//    }

}