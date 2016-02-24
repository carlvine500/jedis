package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JedisPoolConfig extends GenericObjectPoolConfig {
  public JedisPoolConfig() {
    // defaults to make your life with connection pool easier :)
    setTestWhileIdle(true);
    setMinEvictableIdleTimeMillis(60000);
    setTimeBetweenEvictionRunsMillis(30000);
    setNumTestsPerEvictionRun(-1);
  }

  /**
   * please use setMaxTotal(...)
   * @param maxActive
   */
  @Deprecated
  public void setMaxActive(int maxActive) {
    setMaxTotal(maxActive);
  }

  /**
   * please use setMaxWaitMillis(...)
   * @param maxWait
   */
  @Deprecated
  public void setMaxWait(long maxWait) {
    setMaxWaitMillis(maxWait);
  }
}
