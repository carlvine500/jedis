package redis.clients.jedis.loadbanlance;

import redis.clients.jedis.JedisClusterInfoCache.Sharding;
import redis.clients.jedis.JedisPool;

public interface ClusterLoadBanlance {
  public JedisPool getReadJedisPool(Sharding sharding);
}
