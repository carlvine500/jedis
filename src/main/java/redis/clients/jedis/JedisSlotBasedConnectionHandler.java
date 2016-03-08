package redis.clients.jedis;

import java.util.List;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisClusterCommand.Operation;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisSlotBasedConnectionHandler extends JedisClusterConnectionHandler {

  public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig poolConfig, int timeout) {
    this(nodes, poolConfig, timeout, timeout);
  }

  public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout) {
    super(nodes, poolConfig, connectionTimeout, soTimeout);
  }

  public Jedis getConnection() {
    // In antirez's redis-rb-cluster implementation,
    // getRandomConnection always return valid connection (able to
    // ping-pong)
    // or exception if all connections are invalid

    List<JedisPool> pools = cache.getShuffledMasterNodesPool();
    for (JedisPool pool : pools) {
      Jedis jedis = null;
      try {
        jedis = pool.getResource();
        if (jedis == null) {
          continue;
        }
        String result = jedis.ping();
        if (result.equalsIgnoreCase("pong")) return jedis;
        jedis.close();
      } catch (JedisConnectionException ex) {
        if (jedis != null) {
          jedis.close();
        }
      }
    }

    throw new JedisConnectionException("no reachable node in cluster");
  }

  @Override
  public Jedis getConnectionFromSlot(Operation op, int slot) {
    // JedisPool connectionPool = cache.getSlotPool(slot);
    JedisPool connectionPool = null;
    // TODO read only
    if (op == Operation.READONLY) {
      connectionPool = cache.getMasterOrSlaveByWeight(slot);
    } else {
      connectionPool = cache.getMaster(slot);
    }
    if (connectionPool != null) {
      // It can't guaranteed to get valid connection because of node
      // assignment
      Jedis resource = connectionPool.getResource();
      return resource;
    } else {
      return getConnection();
    }
  }

}
