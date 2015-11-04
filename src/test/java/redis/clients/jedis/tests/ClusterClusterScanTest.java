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
import redis.clients.jedis.ScanClusterResult;
import redis.clients.jedis.ScanParams;

public class ClusterClusterScanTest {

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
    jedisClusterNode.add(new HostAndPort("10.126.53.246", 8300));

    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    final JedisCluster jc = new JedisCluster(jedisClusterNode, poolConfig);
    for (int i = 0; i < 1000; i++) {
      // jc.set(i + "", "x");
    }
    ScanParams param = new ScanParams().match("*");
    ScanClusterResult scanClusterKeys = jc.scanClusterKeys(null, ScanParams.SCAN_POINTER_START,
      param);
    int sum = 0;
    while (scanClusterKeys != null) {
      sum += scanClusterKeys.getResult().size();
      System.out.println(scanClusterKeys.getClusterCursor() + "==>" + scanClusterKeys.getResult());
      scanClusterKeys = jc.scanClusterKeys(scanClusterKeys.getClusterCursor(),
        scanClusterKeys.getNodeCursor(), param);

    }
    System.out.println(sum);

    // final byte[][] bytes = new byte[][] { { 50 }, { 51 }, { 52 } };
    // final byte[] bytes2 = new byte[5];
    // jc.setReadWeight(1, 1);
    // // Thread.sleep(1000000000000L);
    // // String s = jc.get("b");
    // for (int i = 0; i < 100000; i++) {
    // String string = jc.get("b");
    // System.out.println(string);
    // }
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
