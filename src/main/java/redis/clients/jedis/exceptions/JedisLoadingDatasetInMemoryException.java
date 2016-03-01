package redis.clients.jedis.exceptions;

public class JedisLoadingDatasetInMemoryException extends JedisConnectionException {

  private static final long serialVersionUID = 1L;

  public JedisLoadingDatasetInMemoryException(String message) {
    super(message);
  }
}
