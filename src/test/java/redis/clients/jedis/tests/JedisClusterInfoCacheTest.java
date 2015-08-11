package redis.clients.jedis.tests;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClusterInfoCache;

public class JedisClusterInfoCacheTest {
  public static final String clusterNodes = ""//
      + "0aba8ecd340d30b66112e7c1f4d181c944a000c6 10.58.22.220:7005 slave 5ce4c4059d5084ae6d83946dcdb21c30460378e4 0 1435831859450 4 connected\n"//
      + "724d0e9c9958b8f113516d265f146e45cacfbc17 10.58.22.220:7001 master - 0 1435831859050 3 connected 5461-10921\n"//
      + "f8eb9f7bf1a453569e916150134dab0db4fc3e64 10.58.22.220:7004 slave 724d0e9c9958b8f113516d265f146e45cacfbc17 0 1435831860050 3 connected\n"//
      + "b70392b533de7ce5d46bb0766278b970ca378d72 10.58.22.220:7003 slave 2ed9344b7ea92e7c26b7d72af3f1a78e21203ce6 0 1435831859650 5 connected\n"//
      + "2ed9344b7ea92e7c26b7d72af3f1a78e21203ce6 10.58.22.220:7000 myself,master - 0 0 1 connected 0-5460\n"//
      + "5ce4c4059d5084ae6d83946dcdb21c30460378e4 10.58.22.220:7002 master - 0 1435831859050 2 connected 10922-16383 [10922->-724d0e9c9958b8f113516d265f146e45cacfbc17] [5461-<-724d0e9c9958b8f113516d265f146e45cacfbc17]\n";

  @Test
  public void reloadSlotShardings() {
    JedisClusterInfoCache cache = new JedisClusterInfoCache(new GenericObjectPoolConfig(), 5000);
    long end = System.nanoTime();
    long begin = end;
    for (int i = 0; i < 100000; i++) {
      cache.reloadSlotShardings(clusterNodes, new HostAndPort("127.0.0.1", 7000));
    }
    long x = System.nanoTime() - begin;
    double ms = x / 1000.0 / 1000;
    System.out.println(ms);
    Assert.assertTrue("reloadSlotShardings cost too much time .", ms < 2000);
  }
}
