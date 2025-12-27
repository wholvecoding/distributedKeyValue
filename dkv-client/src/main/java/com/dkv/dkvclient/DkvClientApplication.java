package com.dkv.dkvclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DkvClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(DkvClientApplication.class, args);
    }

    // 将 DkvClient 作为 Bean 注入 Spring 容器，KvController 可以自动使用
    @Bean
    public DkvClient dkvClient() {
        return new DkvClient("127.0.0.1:2181");
    }
}
