package com.dkv.dkvclient;

import com.dkv.dkvclient.DkvClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kv")
public class KvController {

    private final DkvClient client;

    public KvController(DkvClient client) {
        this.client = client;
    }

    // 测试接口
    @GetMapping("/")
    public String index() {
        return "DKV Client is running!";
    }

    // POST /api/kv/{key}
    @PostMapping("/{key}")
    public ResponseEntity<String> save(
            @PathVariable String key,
            @RequestBody String value) {

        client.put(key, value);
        return ResponseEntity.ok("OK");
    }

    // GET /api/kv/{key}
    @GetMapping("/{key}")
    public ResponseEntity<String> query(@PathVariable String key) {
        String value = client.get(key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(value);
    }
}
