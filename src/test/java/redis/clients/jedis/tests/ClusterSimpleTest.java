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

public class ClusterSimpleTest {

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
    jedisClusterNode.add(new HostAndPort("10.58.22.220", 7000));

    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    poolConfig.setMinIdle(8);
    poolConfig.setMaxIdle(8);
    poolConfig.setMaxTotal(8);
    poolConfig.setTestWhileIdle(true);
    final JedisCluster jc = new JedisCluster(jedisClusterNode, poolConfig);
    final byte[][] bytes = new byte[][] { { 50 }, { 51 }, { 52 } };
    final byte[] bytes2 = new byte[5];
    jc.setReadWeight(1, 1);
    // Thread.sleep(1000000000000L);
    // String s = jc.get("b");
    for (int i = 0; i < 100000; i++) {
      String string = jc.get("b");
      System.out.println(string);
    }
  }

  public static void testname2() throws Exception {
    testname1();
  }

  public static void testname1() throws Exception {
    if ("key".equals("")) {
      System.out.println(Arrays.toString(new Throwable().getStackTrace()));
    }

  }

}
