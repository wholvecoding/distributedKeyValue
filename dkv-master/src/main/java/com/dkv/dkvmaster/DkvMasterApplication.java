package com.dkv.dkvmaster;

import com.dkv.dkvmaster.cluster.ClusterManager;
import com.dkv.dkvmaster.router.ConsistentHashRouter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Scanner;

@SpringBootApplication
public class DkvMasterApplication {

    public static void main(String[] args) throws Exception {
        // 1. 初始化路由算法
        ConsistentHashRouter router = new ConsistentHashRouter();

        // 2. 启动集群管理器
        ClusterManager clusterManager = new ClusterManager(router);
        clusterManager.start();

        // 3. 模拟测试：我们在控制台输入 Key，看看它被分配到哪里
        // ⚠️ 注意：为了测试效果，你需要手动去 ZK 里创建几个节点，或者等 Member B 启动 DataNode
        System.out.println("输入任何字符串测试路由 (输入 'quit' 退出):");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String key = scanner.next();
            if ("quit".equals(key)) {
                break;
            }

            String targetNode = router.routeNode(key);
            System.out.println("Key [" + key + "] ---> Node [" + targetNode + "]");
        }
    }

}
