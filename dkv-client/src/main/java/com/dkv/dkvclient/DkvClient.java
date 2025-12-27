
package com.dkv.dkvclient;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import kvr.KvRequest;
import kvr.KvResponse;

public class DkvClient {

    private final String host;
    private final int port;
    private Channel channel;

    public DkvClient(String zkAddress) {
        // zkAddress 示例: 127.0.0.1:2181
        String[] arr = zkAddress.split(":");
        this.host = arr[0];
        this.port = Integer.parseInt(arr[1]);
        connect();
    }

    // 连接 Zookeeper 获取路由信息
    private void connect() {
        // 创建一个 NIO 事件循环组，用于处理网络事件（读写、连接等）
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            // 创建 Netty 客户端启动器
            Bootstrap b = new Bootstrap();
            b.group(group) // 设置客户端的 EventLoopGroup
                    .channel(NioSocketChannel.class) // 指定客户端 Channel 类型为 NIO 套接字
                    .handler(new ChannelInitializer<>() { // 初始化客户端 Channel 的处理链
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(), // 序列化 Java 对象发送到服务器
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)), // 反序列化服务器返回的对象
                                    new ClientHandler() // 自定义的处理器，用于处理服务端返回的 KvResponse
                            );
                        }
                    });

            // 异步连接服务器，并等待连接完成
            ChannelFuture future = b.connect(host, port).sync();
            this.channel = future.channel(); // 保存连接的 Channel，供后续发送请求使用
            System.out.println("[Client] Connected to " + host + ":" + port); // 输出连接成功信息
        } catch (Exception e) {
            // 捕获连接失败异常
            throw new RuntimeException("Netty connect failed", e);
        }
    }


    public void put(String key, String value) {
        String targetIp = getTargetIp(key);
        KvRequest req = new KvRequest((byte) 1, key, value.getBytes());
        sendRequest(targetIp, req); // 调用工具方法发送请求
    }

    public String get(String key) {
        String targetIp = getTargetIp(key);
        KvRequest req = new KvRequest((byte) 2, key, null);
        KvResponse resp = sendRequest(targetIp, req);
        if (resp == null || resp.getValue() == null) return null;
        return new String(resp.getValue());
    }

    private String getTargetIp(String key) {
        return "127.0.0.1" + ":" + "9000";
    }

    private KvResponse sendRequest(String targetIp, KvRequest req) {
        String[] arr = targetIp.split(":");
        String host = arr[0];
        int port = Integer.parseInt(arr[1]);

        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            Bootstrap b = new Bootstrap();
            ClientHandler handler = new ClientHandler(); // 每次新建一个 Handler，等待结果
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    handler
                            );
                        }
                    });

            // 同步连接
            ChannelFuture f = b.connect(host, port).sync();
            Channel channel = f.channel();

            // 发送请求
            handler.setWaiting(true);
            channel.writeAndFlush(req);

            // 等待响应
            KvResponse resp = handler.waitResult();
            if (resp != null && resp.getCode() == KvResponse.OK) {
                String value = resp.getValue() == null ? null : new String(resp.getValue());
                // 使用 value
            }

            channel.close().sync(); // 关闭连接
            return resp;

        } catch (Exception e) {
            throw new RuntimeException("Netty RPC failed", e);
        } finally {
            group.shutdownGracefully();
        }


    }

}
