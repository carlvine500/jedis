package redis.clients.jedis;

import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisClusterCommand.Operation;
import redis.clients.jedis.JedisClusterInfoCache.SlotState;
import redis.clients.jedis.exceptions.JedisConnectionException;

public abstract class JedisClusterConnectionHandler {
  protected final JedisClusterInfoCache cache;

  abstract Jedis getConnection();

  abstract Jedis getConnectionFromSlot(Operation op, int slot);

  public Jedis getConnectionFromNode(HostAndPort node) {
    cache.setNodeIfNotExist(node);
    return cache.getNode(JedisClusterInfoCache.getNodeKey(node)).getResource();
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
    for (JedisPool jp : cache.getNodes().values()) {
      Jedis jedis = null;
      try {
        jedis = jp.getResource();
        // cache.discoverClusterSlots(jedis);
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
  }

  public void updateSlotState(int slot, SlotState slotState) {
    cache.updateSlotState(slot, slotState);
  }

  public void closeSlave(String nodeKey) {
    cache.closeSlave(nodeKey);
  }

  public void setReadWeight(int masterReadWeight, int slaveReadWeight) {
    cache.setReadWeight(masterReadWeight, slaveReadWeight);
  }

}
