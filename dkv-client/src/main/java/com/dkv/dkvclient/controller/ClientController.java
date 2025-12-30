package com.dkv.dkvclient.controller;

import com.dkv.dkvclient.client.DkvClient;
import org.springframework.web.bind.annotation.*;
import jakarta.annotation.PostConstruct;

import java.util.Arrays;

@RestController
@RequestMapping("/api/kv")
@CrossOrigin(origins = "*")   // 允许所有来源访问
public class ClientController {

    private DkvClient client;

    @PostConstruct
    public void init() {
        // 初始化 DkvClient，只连接一次
        client = new DkvClient("127.0.0.1:2181");
        try {
            client.connect(); // 拉取 ZooKeeper 路由信息
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 保存键值
    // curl -x "" -X POST "http://127.0.0.1:8082/api/kv/save?key=name&value=hty"
    @PostMapping("/save")
    public String save(@RequestParam String key, @RequestParam String value, @RequestParam Integer repeat) {
        try {
            client.put(key, value.getBytes(),repeat);
            return "Saved key: " + key + ", value: " + value;
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to save key: " + key + ", error: " + e.getMessage();
        }
    }

    // 查询键值
    // curl -x "" "http://127.0.0.1:8082/api/kv/get?key=name"
    @GetMapping("/get")
    public String query(@RequestParam String key) {
        try {
            byte[] value = client.get(key);
            if (value == null) {
                return "Key not found: " + key;
            }
            return new String(value);
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to get key: " + key + ", error: " + e.getMessage();
        }
    }

    // 删除键值
    // curl -x "" -X DELETE "http://127.0.0.1:8082/api/kv/delete?key=name"
    @DeleteMapping("/delete")
    public String delete(@RequestParam String key) {
        try {
            client.delete(key);
            return "Deleted key: " + key;
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to delete key: " + key + ", error: " + e.getMessage();
        }
    }
    @GetMapping("/test")
    public String testIp(@RequestParam String IP, @RequestParam String key) {
        try {
            byte[] value = client.testIP(IP, key);
            if (value == null) {
                return "Key not found: " + key;
            }
            return new String(value);
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to get key: " + key + ", error: " + e.getMessage();
        }
    }
}
