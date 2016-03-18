package redis.clients.jedis.exceptions;

public class JedisLinkDownWithMasterException extends JedisConnectionException {

  private static final long serialVersionUID = 1L;

  public JedisLinkDownWithMasterException(String message) {
    super(message);
  }
}
