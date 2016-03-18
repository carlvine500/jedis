package redis.clients.jedis;

import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisLinkDownWithMasterException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

public abstract class JedisClusterCommand<T> {

  private JedisClusterConnectionHandler connectionHandler;
  private int redirections;
  private ThreadLocal<Jedis> askConnection = new ThreadLocal<Jedis>();
  private Operation op = Operation.READWRITE;

  public JedisClusterCommand(JedisClusterConnectionHandler connectionHandler, int maxRedirections) {
    this.connectionHandler = connectionHandler;
    this.redirections = maxRedirections;
  }

  public enum Operation {
    READONLY, READWRITE
  }

  public JedisClusterCommand(Operation op, JedisClusterConnectionHandler connectionHandler,
      int maxRedirections) {
    this.connectionHandler = connectionHandler;
    this.redirections = maxRedirections;
    this.op = op;
  }

  public abstract T execute(Jedis connection);

  public T run(String key) {
    if (key == null) {
      throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    return runWithRetries(SafeEncoder.encode(key), this.redirections, false, false);
  }

  public T run(int keyCount, String... keys) {
    if (keys == null || keys.length == 0) {
      throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    // For multiple keys, only execute if they all share the
    // same connection slot.
    if (keys.length > 1) {
      int slot = JedisClusterCRC16.getSlot(keys[0]);
      for (int i = 1; i < keyCount; i++) {
        int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
        if (slot != nextSlot) {
          throw new JedisClusterException("No way to dispatch this command to Redis Cluster "
              + "because keys have different slots.");
        }
      }
    }

    return runWithRetries(SafeEncoder.encode(keys[0]), this.redirections, false, false);
  }

  public T runBinary(byte[] key) {
    if (key == null) {
      throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    return runWithRetries(key, this.redirections, false, false);
  }

  public T runBinary(int keyCount, byte[]... keys) {
    if (keys == null || keys.length == 0) {
      throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    }

    // For multiple keys, only execute if they all share the
    // same connection slot.
    if (keys.length > 1) {
      int slot = JedisClusterCRC16.getSlot(keys[0]);
      for (int i = 1; i < keyCount; i++) {
        int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
        if (slot != nextSlot) {
          throw new JedisClusterException("No way to dispatch this command to Redis Cluster "
              + "because keys have different slots.");
        }
      }
    }

    return runWithRetries(keys[0], this.redirections, false, false);
  }

  public T runWithAnyNode() {
    Jedis connection = null;
    try {
      connection = connectionHandler.getConnection();
      return execute(connection);
    } catch (JedisConnectionException e) {
      throw e;
    } finally {
      releaseConnection(connection);
    }
  }

  public ScanClusterResult<T> runWithCursor(final String clusterCursor, final String nodeCursor) {
    Jedis connection = null;
    String nodeKey = clusterCursor;
    int retryTimes = redirections;
    for (int i = 0; i < retryTimes; i++) {
      try {
        if (nodeKey == null || ScanParams.SCAN_POINTER_START.equals(nodeCursor)) {
          nodeKey = connectionHandler.nextMasterNodeKey(clusterCursor);
          if (nodeKey == null) {
            return null;
          }
        }
        connection = connectionHandler.getConnectionFromNode(new HostAndPort(nodeKey));
        T data = execute(connection);
        return ScanClusterResult.of(nodeKey, data);
      } catch (JedisConnectionException e) {
        if (i < retryTimes - 1) {
          continue;
        } else {
          throw e;
        }
      } finally {
        releaseConnection(connection);
      }
    }
    return null;
  }

  private T runWithRetries(byte[] key, int redirections, boolean tryRandomNode, boolean asking) {
    if (redirections <= 0) {
      throw new JedisClusterMaxRedirectionsException("Too many Cluster redirections?");
    }

    Jedis connection = null;
    try {

      if (asking) {
        // TODO: Pipeline asking with the original command to make it
        // faster....
        connection = askConnection.get();
        connection.asking();

        // if asking success, reset asking flag
        asking = false;
      } else {
        if (tryRandomNode) {
          connection = connectionHandler.getConnection();
        } else {
          connection = connectionHandler.getConnectionFromSlot(//
            this.redirections == redirections ? op : Operation.READWRITE,//
            JedisClusterCRC16.getSlot(key));
        }
      }

      return execute(connection);
    } catch (JedisConnectionException jce) {
      if (tryRandomNode) {
        // maybe all connection is down
        throw jce;
      }

      // release current connection before recursion
      releaseConnection(connection);

      connection = null;
      if (jce instanceof JedisLinkDownWithMasterException) {
        this.connectionHandler.renewSlotCache();
        return runWithRetries(key, redirections - 1, false, asking);
      }
      // retry with random connection
      return runWithRetries(key, redirections - 1, true, asking);
    } catch (JedisRedirectionException jre) {
      // if MOVED redirection occurred,
      if (jre instanceof JedisMovedDataException) {
        // it rebuilds cluster's slot cache
        // recommended by Redis cluster specification
        this.connectionHandler.renewSlotCache(connection);
      }

      // release current connection before recursion or renewing
      releaseConnection(connection);
      connection = null;

      if (jre instanceof JedisAskDataException) {
        asking = true;
        // migrating just occured in master
        askConnection.set(this.connectionHandler.getConnectionFromNode(jre.getTargetNode()));
      } else if (jre instanceof JedisMovedDataException) {
      } else {
        throw new JedisClusterException(jre);
      }
      return runWithRetries(key, redirections - 1, false, asking);
    } finally {
      releaseConnection(connection);
    }
  }

  private void releaseConnection(Jedis connection) {
    if (connection != null) {
      connection.close();
    }
  }

}
