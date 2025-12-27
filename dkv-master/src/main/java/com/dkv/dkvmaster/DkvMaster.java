package com.dkv.dkvmaster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class DkvMaster {

    private static final String ZK_ADDR = "127.0.0.1:2181";
    private static final String MASTER_PATH = "/dkv/master";
    private static final String MASTER_ADDR = "127.0.0.1:9000";

    private ZooKeeper zk;

    public static void main(String[] args) throws Exception {
        new DkvMaster().start();
    }

    public void start() throws Exception {
        connectZk();
        registerMaster();
        System.out.println("Master started at " + MASTER_ADDR);

        // 阻塞，防止程序退出
        Thread.sleep(Long.MAX_VALUE);
    }

    private void connectZk() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        zk = new ZooKeeper(ZK_ADDR, 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("Connected to ZooKeeper");
                latch.countDown();
            }
        });

        latch.await();
    }

    private void registerMaster() throws Exception {
        // 确保 /dkv 存在
        Stat stat = zk.exists("/dkv", false);
        if (stat == null) {
            zk.create("/dkv", new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }

        // 如果 master 已存在，先删除（测试阶段方便）
        Stat masterStat = zk.exists(MASTER_PATH, false);
        if (masterStat != null) {
            zk.delete(MASTER_PATH, -1);
        }

        // 创建临时节点
        zk.create(
                MASTER_PATH,
                MASTER_ADDR.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL
        );

        System.out.println("Master registered at " + MASTER_ADDR);
    }
}
