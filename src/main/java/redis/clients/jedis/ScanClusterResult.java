package redis.clients.jedis;

import java.util.List;

public class ScanClusterResult {
  public static final String SCAN_POINTER_START = "0";
  public static final String SCAN_POINTER_END = "-1";
  private String clusterCursor;
  private ScanResult<String> scanResult;

  public ScanClusterResult(String clusterCursor, ScanResult<String> scanResult) {
    this.clusterCursor = clusterCursor;
    this.scanResult = scanResult;
  }

  public String getClusterCursor() {
    return clusterCursor;
  }

  public String getNodeCursor() {
    return scanResult.getCursor();
  }

  public List<String> getResult() {
    return scanResult.getResult();
  }

  public ScanResult<String> getScanResult() {
    return scanResult;
  }
}
