package com.dkv.dkvclient;

import com.dkv.dkvclient.client.DkvClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;

@SpringBootApplication
public class DkvClientApplication {

    public static void main(String[] args) {
        // 启动 Spring Boot Web 应用
        DkvClient client = new DkvClient("127.0.0.1:2181");
//        try {
//            client.put("name", "hty".getBytes());
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println("Put finished");

        SpringApplication.run(DkvClientApplication.class, args);
    }
}
