package com.dkv.dkvstorage.config;

import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "datanode.agent")
public class DataNodeAgentConfig {

    private int port = 8081;
    private int bossThreads = 1;
    private int workerThreads = 4;
    private int maxContentLength = 65536;
    private int shutdownTimeout = 30;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getBossThreads() {
        return bossThreads;
    }

    public void setBossThreads(int bossThreads) {
        this.bossThreads = bossThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public int getMaxContentLength() {
        return maxContentLength;
    }

    public void setMaxContentLength(int maxContentLength) {
        this.maxContentLength = maxContentLength;
    }

    public int getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(int shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    @Bean
    public EventLoopGroup bossGroup() {
        return new NioEventLoopGroup(bossThreads);
    }

    @Bean
    public EventLoopGroup workerGroup() {
        return new NioEventLoopGroup(workerThreads);
    }

    @Bean
    public Map<ChannelOption<?>, Object> channelOptions() {
        Map<ChannelOption<?>, Object> options = new HashMap<>();
        options.put(ChannelOption.SO_BACKLOG, 128);
        options.put(ChannelOption.SO_KEEPALIVE, true);
        options.put(ChannelOption.TCP_NODELAY, true);
        return options;
    }
}