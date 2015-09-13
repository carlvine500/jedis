package redis.clients.jedis.exceptions;

public class JedisTargetKeyNameExistsException extends JedisDataException {
  private static final long serialVersionUID = 1L;

  public JedisTargetKeyNameExistsException(String message) {
    super(message);
  }
}
