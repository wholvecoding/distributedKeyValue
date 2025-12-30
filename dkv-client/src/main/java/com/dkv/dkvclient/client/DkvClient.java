package com.dkv.dkvclient.client;

import com.dkv.dkvcommon.model.KvMessage;
import com.dkv.dkvstorage.KvClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.Watcher;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DkvClient {

    private final String zkAddress;  // ZooKeeper 地址
    private final List<String> nodes = new ArrayList<>();
    private CuratorFramework zkClient;

    public DkvClient(String zkAddress) {
        this.zkAddress = zkAddress;
        try {
            this.connect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** 连接 ZooKeeper 并初始化路由表 */
    public void connect() throws Exception {
        zkClient = CuratorFrameworkFactory.newClient(
                zkAddress, new ExponentialBackoffRetry(1000, 3));
        zkClient.start();

        updateNodes();

        // 注册 Watcher，节点变化自动更新
        zkClient.getChildren().usingWatcher((Watcher) event -> {
                    try {
                        updateNodes();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .forPath("/dkv/nodes");
    }

    /** 拉取 /dkv/nodes 下节点列表 */
    private void updateNodes() throws Exception {
        List<String> children = zkClient.getChildren().forPath("/dkv/nodes");
        synchronized (nodes) {
            nodes.clear();
            nodes.addAll(children);
        }
        System.out.println("[DkvClient] Current nodes: " + nodes);
    }

    /** 简单一致性哈希计算目标节点 */
    private String getTargetIp(String key) {
        String url = "http://localhost:8081/api/route?key=" + key;

        RestTemplate restTemplate = new RestTemplate();
        Map<String, Object> response = restTemplate.getForObject(url, Map.class);

        if (response == null || !response.containsKey("primary")) {
            return null;
        }

        return (String) response.get("primary");
    }

    /** PUT 操作 */
    public String put(String key, byte[] value) throws InterruptedException {
        KvMessage message = new KvMessage(KvMessage.Type.PUT, key, value);
        String primaryNode = getTargetIp(key);
        System.out.println("向节点{}发送请求"+primaryNode);
        sendRequest(primaryNode, message);
        return primaryNode;
    }

    /** GET 操作 */
    public byte[] get(String key) throws InterruptedException {
        KvMessage request = new KvMessage(KvMessage.Type.GET, key, null);
        KvMessage response = sendRequest(getTargetIp(key), request);
        return response != null ? response.getValue() : null;
    }

    /** DELETE 操作 */
    public void delete(String key) throws InterruptedException {
        KvMessage message = new KvMessage(KvMessage.Type.DELETE, key, null);
        sendRequest(getTargetIp(key), message);
    }

    /** 使用 Netty 发送请求并返回响应 */
    private KvMessage sendRequest(String nodeIp, KvMessage request) throws InterruptedException {
        String[] parts = nodeIp.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            ClientHandler handler = new ClientHandler();
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
            ChannelFuture future = b.connect(host, port).sync();
            CompletableFuture<KvMessage> responseFuture = new CompletableFuture<>();

// 添加 handler 时，把 responseFuture 传入 handler
            future.channel().pipeline().addLast(new KvClientHandler(responseFuture));

// 发送请求
            future.channel().writeAndFlush(request).sync();
            System.out.println("Request sent, waiting for response...");

            try {
                // 等待响应，最多 5 秒
                KvMessage response = responseFuture.get(1, TimeUnit.SECONDS);
                System.out.println("Got response: " + response);
            } catch (TimeoutException e) {
                System.out.println("Response timed out, closing channel...");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // 超时或正常都要安全关闭 channel
                future.channel().close().sync();
                future.channel().closeFuture().sync();
            }


            return handler.getResponse();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            group.shutdownGracefully();
        }
    }
    public byte[] testIP(String IP, String key) throws InterruptedException {
        KvMessage request = new KvMessage(KvMessage.Type.GET, key, null);
        KvMessage response = sendRequest(IP, request);
        return response != null ? response.getValue() : null;
    }
}
