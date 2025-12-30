package com.dkv.dkvmaster;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.dkv.dkvmaster", "com.dkv.dkvstorage"})
public class DkvMasterApplication {

    public static void main(String[] args) {
        SpringApplication.run(DkvMasterApplication.class, args);
    }
}