package com.dkv.dkvstorage;

import com.dkv.dkvstorage.controller.DataNodeController;
import com.dkv.dkvstorage.rocksdb.DataNodeManager;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ManualTest {

    public static void main(String[] args) throws IOException {
        System.out.println("=== 测试DataNodeController ===");

        // 1. 创建DataNodeManager（需要先创建这个类或模拟）
        DataNodeManager dataNodeManager = new DataNodeManager();

        // 2. 创建Controller实例
        DataNodeController controller = new DataNodeController();

        // 3. 手动设置依赖（因为不能使用@Autowired）
        // 这里需要使用反射或修改Controller代码
        // 方法A：添加setter方法（修改Controller）
        // 方法B：使用反射（如下）
        setPrivateField(controller, "dataNodeManager", dataNodeManager);

        // 4. 测试各个方法
        testStartDataNode1(controller);
        try {
            Thread.sleep(400);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
        }
        testStartDataNode2(controller);
        try {
            Thread.sleep(400);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
        }
        testGetNodeStatus(controller);
        try {
            Thread.sleep(400);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
        }
        testGetAllNodes(controller);
        try {
            Thread.sleep(400);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
        }
//        testStopDataNode1(controller);
//        try {
//            Thread.sleep(400);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt(); // 恢复中断状态
//        }

        testStopDataNode2(controller);


        dataNodeManager.shutdown();
    }

    private static void setPrivateField(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void testStartDataNode1(DataNodeController controller) {
        System.out.println("\n=== 测试startDataNode1 ===");
        Map<String, Object> request = new HashMap<>();
        request.put("nodeId", "node1");
        request.put("host", "127.0.0.1");
        request.put("port", 9000);
        request.put("dataDir", "/dkv/datanode1");
        request.put("isPrimary", true);
        request.put("replicas", "127.0.0.1:9001,127.0.0.1:9002");
        ResponseEntity<Map<String, Object>> response = controller.startDataNode(request);
        System.out.println("响应: " + response.getBody());
    }

    private static void testStartDataNode2(DataNodeController controller) {
        System.out.println("\n=== 测试startDataNode2 ===");
        Map<String, Object> request = new HashMap<>();
        request.put("nodeId", "node2");
        request.put("host", "127.0.0.1");
        request.put("port", 9001);
        request.put("dataDir", "/dkv/datanode2");
        request.put("isPrimary", false);
        request.put("replicas", "127.0.0.1:9000,127.0.0.1:9002");
        ResponseEntity<Map<String, Object>> response = controller.startDataNode(request);
        System.out.println("响应: " + response.getBody());
    }

    private static void testStopDataNode1(DataNodeController controller) {
        System.out.println("\n=== 测试stopDataNode ===");
        Map<String, Object> request = new HashMap<>();
        request.put("nodeId", "node1");

        ResponseEntity<Map<String, Object>> response = controller.stopDataNode(request);
        System.out.println("响应: " + response.getBody());
    }

    private static void testStopDataNode2(DataNodeController controller) {
        System.out.println("\n=== 测试stopDataNode ===");
        Map<String, Object> request = new HashMap<>();
        request.put("nodeId", "node2");

        ResponseEntity<Map<String, Object>> response = controller.stopDataNode(request);
        System.out.println("响应: " + response.getBody());
    }

    private static void testGetNodeStatus(DataNodeController controller) {
        System.out.println("\n=== 测试getNodeStatus ===");
        ResponseEntity<Map<String, Object>> response = controller.getNodeStatus("node1");
        System.out.println("响应: " + response.getBody());
    }

    private static void testGetAllNodes(DataNodeController controller) {
        System.out.println("\n=== 测试getAllNodes ===");
        ResponseEntity<Map<String, Object>> response = controller.getAllNodes();
        System.out.println("响应: " + response.getBody());
    }
}