package redis.clients.jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisClusterCommand.Operation;
import redis.clients.util.ClusterNodeInformation;
import redis.clients.util.ClusterNodeInformation.NodeFlag;
import redis.clients.util.ClusterNodeInformationParser;

public class JedisClusterInfoCache {
  /** ConcurrentHashMap<nodeId,ClusterNodeInformation> */
  private static final ConcurrentHashMap<String, ClusterNodeInformation> nodeInfomations = new ConcurrentHashMap<String, ClusterNodeInformation>();
  /** ConcurrentHashMap<host:port,Object> */
  private static final ConcurrentHashMap<String, JedisPool> nodes = new ConcurrentHashMap<String, JedisPool>();
  /** HashMap<slot,Sharding> */
  private final Map<Integer, Sharding> slotShardings;

  private final GenericObjectPoolConfig poolConfig;
  private int connectionTimeout;
  private int soTimeout;
  private int masterReadWeight = 1;
  private int slaveReadWeight = 0;

  public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig, int timeout) {
    this(poolConfig, timeout, timeout);
  }

  public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig,
      final int connectionTimeout, final int soTimeout) {
    this.poolConfig = poolConfig;
    this.connectionTimeout = connectionTimeout;
    this.soTimeout = soTimeout;
    slotShardings = new HashMap<Integer, Sharding>(BinaryJedisCluster.HASHSLOTS, 1F);
    for (int i = 0; i < BinaryJedisCluster.HASHSLOTS; i++) {
      slotShardings.put(i, new Sharding());
    }
  }

  public void setNodeIfNotExist(HostAndPort node) {
    setNodeIfNotExist(node, Operation.READWRITE);
  }

  private void setNodeIfNotExist(HostAndPort node, Operation op) {
    String nodeKey = node.getNodeKey();
    if (nodes.containsKey(nodeKey)) {
      JedisFactory jf = (JedisFactory) nodes.get(nodeKey).getInternalPool().getFactory();
      // when M/S switch , slaves need to be readonly mode;
      if (jf.getOperation() == op) {
        return;
      }
    }

    JedisPool nodePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
        connectionTimeout, soTimeout, null, 0, null, op);
    JedisPool oldJedisPool = nodes.get(nodeKey);
    nodes.put(nodeKey, nodePool);
    if (oldJedisPool != null) {
      oldJedisPool.close();
    }
  }

  public JedisPool getNode(String nodeKey) {
    return nodes.get(nodeKey);
  }

  public Map<String, JedisPool> getNodes() {
    return nodes;
  }

  public void reloadSlotShardings(Jedis jedis) {
    String clusterNodes = jedis.clusterNodes();
    HostAndPort current = new HostAndPort(jedis.getClient().getHost(), jedis.getClient().getPort());
    reloadSlotShardings(clusterNodes, current);
  }

  public void reloadSlotShardings(String clusterNodes, HostAndPort current) {
    Map<String, ClusterNodeInformation> nodeInfoMap = new HashMap<String, ClusterNodeInformation>();
    for (String nodeInfo : clusterNodes.split("\n")) {
      ClusterNodeInformation clusterNodeInfo = ClusterNodeInformationParser
          .parse(nodeInfo, current);
      nodeInfoMap.put(clusterNodeInfo.getNodeId(), clusterNodeInfo);
      if (clusterNodeInfo.isDead()) {
        closeSlaveConnection(clusterNodeInfo.getNode().getNodeKey());
        continue;
      }
      if (clusterNodeInfo.isSameSlot(nodeInfomations.get(clusterNodeInfo.getNodeId()))) {
        continue;
      }
      Operation op = null;
      if (clusterNodeInfo.getFlags().contains(NodeFlag.SLAVE)) {
        op = Operation.READONLY;
      }
      if (clusterNodeInfo.getFlags().contains(NodeFlag.MASTER)) {
        op = Operation.READWRITE;
      }
      if (op == null) {
        continue;
      }
      setNodeIfNotExist(clusterNodeInfo.getNode(), op);
    }

    for (ClusterNodeInformation nodeInfo : nodeInfoMap.values()) {
      if (nodeInfo.isSameSlot(nodeInfomations.get(nodeInfo.getNodeId()))) {
        continue;
      }
      nodeInfomations.put(nodeInfo.getNodeId(), nodeInfo);
      List<Integer> availableSlots = nodeInfo.getAvailableSlots();
      ClusterNodeInformation masterNodeInfo = nodeInfo;
      if (nodeInfo.getFlags().contains(NodeFlag.MASTER)) {
        List<Integer> slotsBeingImported = masterNodeInfo.getSlotsBeingImported();
        for (Integer slot : slotsBeingImported) {
          slotShardings.get(slot).setSlotState(SlotState.IMPORTING);
        }
        List<Integer> slotsBeingMigrated = masterNodeInfo.getSlotsBeingMigrated();
        for (Integer slot : slotsBeingMigrated) {
          slotShardings.get(slot).setSlotState(SlotState.MIGRATING);
        }
      }

      if (availableSlots.isEmpty()) {
        masterNodeInfo = nodeInfoMap.get(nodeInfo.getSlaveOf());
        availableSlots = masterNodeInfo.getAvailableSlots();
      }

      JedisPool jedisPool = nodes.get(nodeInfo.getNode().getNodeKey());
      for (Integer slot : availableSlots) {
        if (nodeInfo.getFlags().contains(NodeFlag.MASTER)) {
          slotShardings.get(slot).setMaster(jedisPool);
        } else {
          slotShardings.get(slot).addSlave(jedisPool);
        }
      }

    }
  }

  public JedisPool getMaster(int slot) {
    return slotShardings.get(slot).master;
  }

  public JedisPool getMasterOrSlaveByWeight(int slot) {
    Sharding sharding = slotShardings.get(slot);
    if (sharding.isMigratingImporting()) {
      return sharding.getMaster();
    }
    return getReadJedisPool(sharding);
  }

  public JedisPool getReadJedisPool(Sharding sharding) {
    List<JedisPool> list = sharding.slaves;
    if (list.isEmpty()) {
      return sharding.getMaster();
    }
    int size = list.size();
    int index = randomNumber(masterReadWeight + slaveReadWeight * size);
    // System.out.println(index);
    if (index < masterReadWeight) {
      return sharding.getMaster();
    }
    JedisPool jedisPool = list.get((index - masterReadWeight) / slaveReadWeight);
    if (jedisPool.isClosed()) {
      list.remove(jedisPool);
      return getReadJedisPool(sharding);
    }
    return jedisPool;
  }

  public void setReadWeight(int masterReadWeight, int slaveReadWeight) {
    if ((masterReadWeight + slaveReadWeight) == 0//
        || masterReadWeight < 0//
        || slaveReadWeight < 0) {
      throw new IllegalArgumentException("masterReadWeight slaveReadWeight set error");
    }
    this.masterReadWeight = masterReadWeight;
    this.slaveReadWeight = slaveReadWeight;
  }

  // private final static Random random = new Random();
  private static int randomNumber(int size) {
    // random reference: http://www.oschina.net/question/157182_45274
    return (int) (Math.floor(Math.random() * size));
    // return random.nextInt(size);
  }

  public void setSlotState(int slot, SlotState slotState) {
    slotShardings.get(slot).setSlotState(slotState);
  }

  public void closeSlaveConnection(String nodeKey) {
    nodes.get(nodeKey).close();
  }

  public static class Sharding {
    private volatile transient JedisPool master;
    private List<JedisPool> slaves = new CopyOnWriteArrayList<JedisPool>();
    private volatile transient SlotState slotState = SlotState.STABLE;

    public boolean isMigratingImporting() {
      return slotState != SlotState.STABLE;
    }

    public JedisPool getMaster() {
      return master;
    }

    public void setMaster(JedisPool master) {
      this.master = master;
    }

    public synchronized void addSlave(JedisPool slave) {
      if (!slaves.contains(slave)) {
        slaves.add(slave);
      }
    }

    public List<JedisPool> getSlaves() {
      return slaves;
    }

    public void setSlotState(SlotState slotState) {
      this.slotState = slotState;
    }

    public SlotState getSlotState() {
      return slotState;
    }
  }

  public enum SlotState {
    IMPORTING, MIGRATING, STABLE
  }

}
