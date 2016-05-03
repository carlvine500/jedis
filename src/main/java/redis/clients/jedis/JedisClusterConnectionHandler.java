package redis.clients.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisClusterCommand.Operation;
import redis.clients.jedis.JedisClusterInfoCache.SlotState;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.loadbanlance.ClusterLoadBanlance;

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

  public String nextMasterNodeKey(String currentNodeId) {
    return cache.nextMasterNodeKey(currentNodeId);
  }

  private void initializeSlotsCache(Set<HostAndPort> startNodes, GenericObjectPoolConfig poolConfig) {
    for (HostAndPort hostAndPort : startNodes) {
      Jedis jedis = null;
      try {
        jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
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
    List<JedisPool> pools = cache.getShuffledMasterNodesPool();
    for (JedisPool pool : pools) {
      Jedis jedis = null;
      try {
        jedis = pool.getResource();
        if (jedis == null) {
          continue;
        }
        cache.reloadSlotShardings(jedis);
        break;
      } catch (JedisConnectionException ex) {
        // try next node
      } finally {
        if (jedis != null) {
          jedis.close();
        }
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

  public void setClusterLoadBanlance(ClusterLoadBanlance clusterLoadBanlance) {
    cache.setClusterLoadBanlance(clusterLoadBanlance);
  }

  public void removeClusterLoadBanlance() {
    cache.removeClusterLoadBanlance();
  }

  ScheduledExecutorService pool;

  public void startSlotCacheMonitor() {
    pool = Executors.newSingleThreadScheduledExecutor();
    pool.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        renewSlotCache();
      }
    }, 5, 1, TimeUnit.SECONDS);
  }

  public int getSlavesCount(int slot) {
    return cache.getSlavesCount(slot);
  }

  public void shutdown() {
    pool.shutdown();
  }

}
