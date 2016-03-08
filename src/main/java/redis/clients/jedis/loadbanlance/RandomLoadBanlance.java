package redis.clients.jedis.loadbanlance;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import redis.clients.jedis.JedisClusterInfoCache.Sharding;
import redis.clients.jedis.JedisPool;

public class RandomLoadBanlance implements ClusterLoadBanlance {
  private volatile int masterReadWeight = 1;
  private volatile int slaveReadWeight = 0;

  public RandomLoadBanlance() {
  }

  public RandomLoadBanlance(int masterReadWeight, int slaveReadWeight) {
    setReadWeight(masterReadWeight, slaveReadWeight);
  }

  public JedisPool getReadJedisPool(Sharding sharding) {
    List<JedisPool> list = sharding.getSlaves();
    if (list.isEmpty()) {
      return sharding.getMaster();
    }
    int size = list.size();
    int index = RandomUtils.nextInt(0, masterReadWeight + slaveReadWeight * size);
    if (index < masterReadWeight) {
      return sharding.getMaster();
    }
    JedisPool jedisPool = list.get((index - masterReadWeight) / slaveReadWeight);
    if (jedisPool.isClosed()) {
      list.remove(jedisPool);
      return getReadJedisPool(sharding);
    }
    return jedisPool;
  }

  public void setReadWeight(int masterReadWeight, int slaveReadWeight) {
    if ((masterReadWeight + slaveReadWeight) == 0//
        || masterReadWeight < 0//
        || slaveReadWeight < 0) {
      throw new IllegalArgumentException("masterReadWeight slaveReadWeight set error");
    }
    this.masterReadWeight = masterReadWeight;
    this.slaveReadWeight = slaveReadWeight;
  }
}
