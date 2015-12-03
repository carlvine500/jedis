package redis.clients.jedis;

public class ScanClusterResult<T> {
  private String clusterCursor;
  private T data;

  public static <T> ScanClusterResult<T> of(String clusterCursor, T data) {
    return new ScanClusterResult<T>(clusterCursor, data);
  }

  public ScanClusterResult(String clusterCursor, T data) {
    this.clusterCursor = clusterCursor;
    this.data = data;
  }

  public String getClusterCursor() {
    return clusterCursor;
  }

  public T getData() {
    return data;
  }

}
