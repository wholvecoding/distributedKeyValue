package com.dkv.dkvmaster;

import com.dkv.dkvmaster.cluster.ClusterManager;
import com.dkv.dkvmaster.router.ConsistentHashRouter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Scanner;

@SpringBootApplication(scanBasePackages = {"com.dkv.dkvmaster", "com.dkv.dkvstorage"})
public class DkvMasterApplication implements CommandLineRunner {

    private final ClusterManager clusterManager;
    private final ConsistentHashRouter router;

    public DkvMasterApplication() {
        this.router = new ConsistentHashRouter();
        this.clusterManager = new ClusterManager(router);
    }

    public static void main(String[] args) {
        SpringApplication.run(DkvMasterApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        clusterManager.start();

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
