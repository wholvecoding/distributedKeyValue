package com.dkv.dkvclient.client;

import com.dkv.dkvcommon.model.KvMessage;
import com.dkv.dkvstorage.KvClientHandler;
import com.dkv.dkvstorage.rocksdb.DataNode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

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
        logger.warn((String) response.get("primary")+"上的key:"+key+"已经被删除了");
        return (String) response.get("primary");
    }
    private Map<String, Object> getRouteData(String key,Integer repeat) {
        String url = "http://localhost:8081/api/route?key=" + key+"&replicas=" + repeat;

        RestTemplate restTemplate = new RestTemplate();
        Map<String, Object> response = restTemplate.getForObject(url, Map.class);

        if (response == null || !response.containsKey("primary")) {
            return null;
        }

        // 直接返回解析后的 Map，或者你可以自己在这里过滤一下只返回这两个字段
        return response;
    }

    /** PUT 操作 */
    public String put(String key, byte[] value,Integer repeat) throws InterruptedException {
        KvMessage message = new KvMessage(KvMessage.Type.PUT, key, value);
        Map<String, Object> data = getRouteData(key,repeat);
        if (data != null) {
            String primary = (String) data.get("primary");
            List<String> replicas = (List<String>) data.get("replicas"); // 需要强转
            logger.info("当前的replicas  "+replicas);
            String replicasParam = String.join(",", replicas);
            String url = "http://localhost:8085/agent/set?nodeId=" +primary +"&replicas=" + replicasParam;

            RestTemplate restTemplate = new RestTemplate();
            Map<String, Object> response1 = restTemplate.getForObject(url, Map.class);
            logger.info("向节点 "+primary+"发送请求");
            sendRequest(primary, message);
            return primary;
            // ...
        }else{
            return "error";
        }

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
    private static final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private KvMessage sendRequest(String nodeIp, KvMessage request) {
        String[] parts = nodeIp.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        // 1. 创建 Future 用于接收结果
        CompletableFuture<KvMessage> responseFuture = new CompletableFuture<>();

        Bootstrap b = new Bootstrap();
        b.group(workerGroup) // 复用全局线程池
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000) // 连接超时
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                                new ObjectEncoder(),
                                new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                // 2. 直接在这里把 future 传给 Handler
                                new SimpleChannelInboundHandler<KvMessage>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, KvMessage msg) {
                                        // 收到消息，完成 future
                                        responseFuture.complete(msg);
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        // 发生异常，通知 future 失败
                                        responseFuture.completeExceptionally(cause);
                                        ctx.close();
                                    }
                                }
                        );
                    }
                });

        Channel channel = null;
        try {
            // 连接
            ChannelFuture connectFuture = b.connect(host, port).sync();
            channel = connectFuture.channel();

            // 发送请求
            System.out.println("Request sent to " + nodeIp + ", waiting for response...");
            channel.writeAndFlush(request);

            // 3. 等待结果 (等待 Handler 调用 complete)
            KvMessage response = responseFuture.get(5, TimeUnit.SECONDS);
            System.out.println("Got response: " + response);
            return response;

        } catch (TimeoutException e) {
            System.out.println("Response timed out from " + nodeIp);
            throw new RuntimeException("Request timeout", e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Request failed: " + e.getMessage(), e);
        } finally {
            // 4. 关闭连接 (注意：不要关闭 workerGroup，只关闭 channel)
            if (channel != null) {
                channel.close();
            }
        }
    }
    public byte[] testIP(String IP, String key) throws InterruptedException {
        KvMessage request = new KvMessage(KvMessage.Type.GET, key, null);
        KvMessage response = sendRequest(IP, request);
        return response != null ? response.getValue() : null;
    }
}
