package com.dkv.dkvstorage;




import com.dkv.dkvcommon.model.KvMessage;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class ConsistencyTest {

    public static void main(String[] args) throws Exception {

        String key = "k1";

        SimpleKvClient primary =
                new SimpleKvClient("127.0.0.1", 9001);

        SimpleKvClient replica1 =
                new SimpleKvClient("127.0.0.1", 9002);

        SimpleKvClient replica2 =
                new SimpleKvClient("127.0.0.1", 9003);

        // =====================
        // 1. 第一次写
        // =====================
        String v1 = "v1-" + System.currentTimeMillis();

        KvMessage put1 = new KvMessage(
                KvMessage.Type.PUT,
                key,
                v1.getBytes(StandardCharsets.UTF_8)
        );
        put1.setRequestId(UUID.randomUUID().toString());

        KvMessage r1 = primary.send(put1);
        System.out.println("[PUT-1] " + r1.getMessage());

//        Thread.sleep(1500);

        KvMessage get1 = new KvMessage(KvMessage.Type.GET, key, null);
        get1.setRequestId(UUID.randomUUID().toString());

        KvMessage g1 = replica1.send(get1);
        String rv1 = g1.getValue() == null ? null :
                new String(g1.getValue(), StandardCharsets.UTF_8);

        System.out.println("[GET-1] replica value=" + rv1);


        // =====================
        // 2. 第二次写（覆盖）
        // =====================
        String v2 = "v2-" + System.currentTimeMillis();

        KvMessage put2 = new KvMessage(
                KvMessage.Type.PUT,
                key,
                v2.getBytes(StandardCharsets.UTF_8)
        );
        put2.setRequestId(UUID.randomUUID().toString());

        KvMessage r2 = primary.send(put2);
        System.out.println("[PUT-2] " + r2.getMessage());

        // 等待复制
        Thread.sleep(2000);

        KvMessage get2 = new KvMessage(KvMessage.Type.GET, key, null);
        get2.setRequestId(UUID.randomUUID().toString());

        KvMessage g2 = replica2.send(get2);
        String rv2 = g2.getValue() == null ? null :
                new String(g2.getValue(), StandardCharsets.UTF_8);

        System.out.println("[GET-2] replica value=" + rv2);

        KvMessage delete = new KvMessage(
                KvMessage.Type.DELETE,
                key,
                v2.getBytes(StandardCharsets.UTF_8)
        );
        delete.setRequestId(UUID.randomUUID().toString());
        KvMessage dr = primary.send(delete);
        System.out.println(dr.getMessage());

        KvMessage get3 = new KvMessage(
                KvMessage.Type.GET,
                key,
                v2.getBytes(StandardCharsets.UTF_8)
        );
        KvMessage r3 = replica1.send(get3);
        System.out.println(r3.getMessage());
        // =====================
        // 3. 一致性判断
        // =====================
        if(v1.equals(rv1))
        {
            System.out.println("✅ PUT CONSISTENT");
        }else{
            System.out.println("❌ PUT INCONSISTENT");
        }

        if (v2.equals(rv2)) {
            System.out.println("✅ UPDATE CONSISTENT");
        } else {
            System.out.println("❌ UPDATE INCONSISTENT");
        }
    }
}