package redis.clients.jedis;

import java.util.Collection;
import java.util.Set;

import org.apache.commons.lang3.RandomUtils;
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

  private JedisPool getRandomNode() {
    Collection<JedisPool> pools = cache.getNodes().values();
    int nextInt = RandomUtils.nextInt(0, pools.size());
    int i = 0;
    for (JedisPool pool : pools) {
      if (nextInt == i) {
        return pool;
      }
      i++;
    }
    return null;
  }

  public Jedis getConnection() {
    // In antirez's redis-rb-cluster implementation,
    // getRandomConnection always return valid connection (able to
    // ping-pong)
    // or exception if all connections are invalid

    int tryTimes = cache.getNodes().values().size();

    for (int i = 0; i < tryTimes; i++) {
      JedisPool pool = getRandomNode();
      if (pool == null) {
        continue;
      }
      Jedis jedis = null;
      try {
        jedis = pool.getResource();
        if (jedis == null) {
          continue;
        }
        String result = jedis.ping();
        if (result.equalsIgnoreCase("pong")) return jedis;
        pool.returnBrokenResource(jedis);
      } catch (JedisConnectionException ex) {
        if (jedis != null) {
          pool.returnBrokenResource(jedis);
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
