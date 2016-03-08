package redis.clients.jedis.loadbanlance;

import redis.clients.jedis.JedisClusterInfoCache.Sharding;
import redis.clients.jedis.JedisPool;

public class DefaultLoadBanlance implements ClusterLoadBanlance {
  private static final DefaultLoadBanlance lb = new DefaultLoadBanlance();

  private DefaultLoadBanlance() {
  }

  public static DefaultLoadBanlance getSingleton() {
    return lb;
  }

  @Override
  public JedisPool getReadJedisPool(Sharding sharding) {
    return sharding.getMaster();
  }
}
