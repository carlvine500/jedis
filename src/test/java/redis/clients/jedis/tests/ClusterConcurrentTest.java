package redis.clients.jedis.tests;

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
    final byte[] bytes2 = new byte[5];
    String string = jc.get("b");
    System.out.println(string);
    // cluster c400 qps 6.8w
    // s c400 qps 4.7w
    final AtomicInteger i = new AtomicInteger();
    StressTestUtils.testAndPrint(400, 100 * 10000, new StressTask() {
      @Override
      public Object doTask() throws Exception {
        // byte[] bs = jc.get(bytes[i.incrementAndGet() % 3]);
        String bs = jc.get("b");
        return null;
      }
    });
  }

}
