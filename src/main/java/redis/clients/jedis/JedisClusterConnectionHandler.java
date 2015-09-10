package redis.clients.jedis;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisClusterCommand.Operation;
import redis.clients.jedis.JedisClusterInfoCache.SlotState;
import redis.clients.jedis.exceptions.JedisConnectionException;

public abstract class JedisClusterConnectionHandler {
  protected final JedisClusterInfoCache cache;

  abstract Jedis getConnection();

  abstract Jedis getConnectionFromSlot(Operation op, int slot);

  public HostAndPort getMasterHostAndPort(int slot) {
    return cache.getMasterHostAndPort(slot);
  }

  public Jedis getConnectionFromNode(HostAndPort node) {
    cache.setNodeIfNotExist(node);
    return cache.getNode(node.getNodeKey()).getResource();
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout) {
    this.cache = new JedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout);
    initializeSlotsCache(nodes, poolConfig);
  }

  public Map<String, JedisPool> getNodes() {
    return cache.getNodes();
  }

  private void initializeSlotsCache(Set<HostAndPort> startNodes, GenericObjectPoolConfig poolConfig) {
    for (HostAndPort hostAndPort : startNodes) {
      Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
      try {
        // cache.discoverClusterNodesAndSlots(jedis);
        cache.reloadSlotShardings(jedis);
        break;
      } catch (JedisConnectionException e) {
        // try next nodes
      } finally {
        if (jedis != null) {
          jedis.close();
        }
      }
    }

    for (HostAndPort node : startNodes) {
      cache.setNodeIfNotExist(node);
    }
  }

  public void renewSlotCache() {
    Jedis jedis = null;
    try {
      jedis = getConnection();
      // cache.discoverClusterSlots(jedis);
      cache.reloadSlotShardings(jedis);
      // try next nodes
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  public void renewSlotCache(Jedis jedis) {
    try {
      cache.reloadSlotShardings(jedis);
    } catch (JedisConnectionException e) {
      renewSlotCache();
    }
  }

  public void setSlotState(int slot, SlotState slotState) {
    cache.setSlotState(slot, slotState);
  }

  public void closeConnections(String nodeKey) {
    cache.closeConnections(nodeKey);
  }

  public void setReadWeight(int masterReadWeight, int slaveReadWeight) {
    cache.setReadWeight(masterReadWeight, slaveReadWeight);
  }

  private static class SingletonHolder {
    private static final ScheduledExecutorService pool = Executors
        .newSingleThreadScheduledExecutor();
  }

  public void startSlotCacheMonitor() {
    SingletonHolder.pool.scheduleWithFixedDelay(new Runnable() {

      @Override
      public void run() {
        renewSlotCache();
      }
    }, 5, 1, TimeUnit.SECONDS);
  }

}
