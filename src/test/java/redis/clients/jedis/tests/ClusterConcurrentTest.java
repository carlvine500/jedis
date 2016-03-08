package redis.clients.jedis.tests;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.taobao.stresstester.StressTestUtils;
import com.taobao.stresstester.core.StressTask;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class ClusterConcurrentTest {

  /**
   * @param args
   */
  public static void main(String[] args) {
    Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
    jedisClusterNode.add(new HostAndPort("10.58.22.220", 7000));

    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    poolConfig.setMaxIdle(200);
    poolConfig.setMaxTotal(500);
    final JedisCluster jc = new JedisCluster(jedisClusterNode, poolConfig);
    final byte[][] bytes = new byte[][] { { 50 }, { 51 }, { 52 } };
    final byte[] bytes2 = new byte[1000];// 10bytes 41943.887--QPS 1kb 8974.129--QPS 10kb
                                              // 949.2576--QPS avg=421.38193 ms
    final byte[] bytes3 = "b".getBytes();
    jc.set(bytes3, bytes2);
    final byte[] bs = jc.get(bytes3);
    System.out.println(Arrays.toString(bs));
    // cluster c400 qps 6.8w
    // s c400 qps 4.7w
    // final AtomicInteger i = new AtomicInteger();
    StressTestUtils.testAndPrint(400, 10 * 10000, new StressTask() {
      @Override
      public Object doTask() throws Exception {
        // byte[] bs = jc.get(bytes[i.incrementAndGet() % 3]);
        byte[] bs = jc.get(bytes3);
        return null;
      }
    });
  }

}
