package redis.clients.jedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisClusterCommand.Operation;
import redis.clients.jedis.loadbanlance.ClusterLoadBanlance;
import redis.clients.jedis.loadbanlance.DefaultLoadBanlance;
import redis.clients.util.ClusterNodeInformation;
import redis.clients.util.ClusterNodeInformation.NodeFlag;
import redis.clients.util.ClusterNodeInformationParser;

public class JedisClusterInfoCache {
  private ThreadLocal<ClusterLoadBanlance> loadBanlanceHolder = new ThreadLocal<ClusterLoadBanlance>();
  /** HashMap<nodeId,ClusterNodeInformation> */
  private final ConcurrentHashMap<String, ClusterNodeInformation> nodeInfomations = new ConcurrentHashMap<String, ClusterNodeInformation>();
  /** ConcurrentHashMap<host:port,Object> , it's shared by many clusters . */
  private static final ConcurrentHashMap<String, JedisPool> nodes = new ConcurrentHashMap<String, JedisPool>();
  /** HashMap<slot,Sharding> */
  private final Map<Integer, Sharding> slotShardings;
  private final GenericObjectPoolConfig poolConfig;
  private int connectionTimeout;
  private int soTimeout;

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

  public String getCachedNodeInfos() {
    StringBuilder sb = new StringBuilder();
    for (ClusterNodeInformation nodeInfo : nodeInfomations.values()) {
      sb.append(nodeInfo.getNodeId()).append(" ");
      sb.append(nodeInfo.getNode().getNodeKey()).append(" ");
      sb.append(StringUtils.join(nodeInfo.getFlags(), ",")).append(" ");
      sb.append(StringUtils.join(nodeInfo.getSlotRanges(), " ")).append("\r\n");
    }
    return sb.toString();
  }

  public void setClusterLoadBanlance(ClusterLoadBanlance clusterLoadBanlance) {
    loadBanlanceHolder.set(clusterLoadBanlance);
  }

  public void removeClusterLoadBanlance() {
    loadBanlanceHolder.remove();
  }

  public String nextMasterNodeKey(String currentNodeKey) {
    Iterator<ClusterNodeInformation> iterator = nodeInfomations.values().iterator();
    while (iterator.hasNext()) {
      if (StringUtils.isBlank(currentNodeKey)) {
        return nextMasterNodeKey(iterator);
      }
      String tmpNodeId = iterator.next().getNode().getNodeKey();
      if (StringUtils.equals(currentNodeKey, tmpNodeId)) {
        return nextMasterNodeKey(iterator);
      }
    }
    return null;
  }

  private String nextMasterNodeKey(Iterator<ClusterNodeInformation> iterator) {
    while (iterator.hasNext()) {
      ClusterNodeInformation tmpNodeInfo = iterator.next();
      if (tmpNodeInfo.isMaster() && tmpNodeInfo.isActive()) {
        return tmpNodeInfo.getNode().getNodeKey();
      }
    }
    return null;
  }

  /**
   * after setting a master readOnly , master will still accept write command .
   */
  public void setNodeIfNotExist(HostAndPort node) {
    setNodeIfNotExist(node, Operation.READONLY);
  }

  private void setNodeIfNotExist(HostAndPort node, Operation op) {
    String nodeKey = node.getNodeKey();
    if (nodes.containsKey(nodeKey) && !nodes.get(nodeKey).isClosed()) {
      // JedisFactory jf = (JedisFactory) nodes.get(nodeKey).getInternalPool().getFactory();
      // // when M/S switch , slaves need to be readonly mode, readwrite will exe by redis master;
      // if (jf.getOperation() != op) {
      // jf.setOperation(jf.getOperation() == Operation.READONLY ? Operation.READWRITE
      // : Operation.READONLY);
      // }
      return;
    }

    JedisPool nodePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
        connectionTimeout, soTimeout, null, 0, null, op);
    nodes.put(nodeKey, nodePool);
  }

  public JedisPool getNode(String nodeKey) {
    return nodes.get(nodeKey);
  }

  public Map<String, JedisPool> getNodes() {
    Collection<ClusterNodeInformation> values = nodeInfomations.values();
    Map<String, JedisPool> jedisPools = new HashMap<String, JedisPool>();
    for (ClusterNodeInformation nodeInfo : values) {
      String nodeKey = nodeInfo.getNode().getNodeKey();
      jedisPools.put(nodeKey, nodes.get(nodeKey));
    }
    return jedisPools;
  }

  public List<JedisPool> getShuffledMasterNodesPool() {
    Collection<ClusterNodeInformation> values = nodeInfomations.values();
    List<JedisPool> list = new ArrayList<JedisPool>();
    for (ClusterNodeInformation nodeInfo : values) {
      if (!nodeInfo.isMaster() || nodeInfo.getSlotRanges().length == 0) {
        continue;
      }
      String nodeKey = nodeInfo.getNode().getNodeKey();
      list.add(nodes.get(nodeKey));
    }
    Collections.shuffle(list);
    return list;
  }

  public void reloadSlotShardings(Jedis jedis) {
    String clusterNodes = jedis.clusterNodes();
    HostAndPort current = new HostAndPort(jedis.getClient().getHost(), jedis.getClient().getPort());
    reloadSlotShardings(clusterNodes, current);
  }

  public synchronized void reloadSlotShardings(String clusterNodes, HostAndPort current) {
    Map<String, ClusterNodeInformation> nodeInfoMap = ClusterNodeInformationParser.parseAll(
      clusterNodes, current);
    boolean canReloadSlotShardings = false;

    for (ClusterNodeInformation newNodeInfo : nodeInfoMap.values()) {
      newNodeInfo.getFlags().remove(NodeFlag.MYSELF);
      ClusterNodeInformation oldNodeInfo = nodeInfomations.get(newNodeInfo.getNodeId());
      if (newNodeInfo.isInactive()) {
        nodeInfomations.remove(newNodeInfo.getNodeId());
        closeConnections(newNodeInfo.getNode().getNodeKey());
        continue;
      }
      nodeInfomations.put(newNodeInfo.getNodeId(), newNodeInfo);
      if (newNodeInfo.isSameSlot(oldNodeInfo) && newNodeInfo.isSameMaster(oldNodeInfo)
          && newNodeInfo.isSameFlags(oldNodeInfo)) {
        continue;
      }
      if (newNodeInfo.isMaster() && newNodeInfo.getSlotRanges().length == 0) {
        continue;
      }
      setNodeIfNotExist(newNodeInfo.getNode(), Operation.READONLY);
      canReloadSlotShardings = true;
    }

    if (!canReloadSlotShardings) {
      return;
    }

    Map<String, List<String>> masterSlaveNodeIds = ClusterNodeInformationParser
        .getMasterSlaveNodeIds(nodeInfoMap);
    for (Entry<String, List<String>> masterSlaveNodeIdEntry : masterSlaveNodeIds.entrySet()) {
      String masterNodeId = masterSlaveNodeIdEntry.getKey();
      ClusterNodeInformation masterNodeInfo = nodeInfoMap.get(masterNodeId);
      JedisPool masterJedisPool = nodes.get(masterNodeInfo.getNode().getNodeKey());

      List<String> slaveNodeIds = masterSlaveNodeIdEntry.getValue();
      List<JedisPool> slaveJedisPools = new ArrayList<JedisPool>(slaveNodeIds.size());
      for (String slaveNodeId : slaveNodeIds) {
        JedisPool slaveJedisPool = nodes.get(nodeInfoMap.get(slaveNodeId).getNode().getNodeKey());
        slaveJedisPools.add(slaveJedisPool);
      }

      CopyOnWriteArrayList<JedisPool> slavePools = new CopyOnWriteArrayList<JedisPool>(
          slaveJedisPools);
      for (Integer slot : masterNodeInfo.getAvailableSlots()) {
        Sharding sharding = slotShardings.get(slot);
        sharding.setMaster(masterJedisPool);
        sharding.setSlaves(slavePools);
      }
    }
  }

  public JedisPool getMaster(int slot) {
    return slotShardings.get(slot).master;
  }

  public JedisPool tryGetOneSlave(int slot) {
    Sharding sharding = slotShardings.get(slot);
    List<JedisPool> list = sharding.getSlaves();
    if (list.isEmpty()) {
      return sharding.getMaster();
    }
    return list.get(RandomUtils.nextInt(0, list.size()));
  }

  public HostAndPort getMasterHostAndPort(int slot) {
    return ((JedisFactory) getMaster(slot).getInternalPool().getFactory()).getHostAndPort();
  }

  public int getSlavesCount(int slot) {
    return slotShardings.get(slot).slaves.size();
  }

  public JedisPool getMasterOrSlaveByWeight(int slot) {
    Sharding sharding = slotShardings.get(slot);
    if (sharding.isMigratingImporting()) {
      return sharding.getMaster();
    }
    return getReadJedisPool(sharding);
  }

  public JedisPool getReadJedisPool(Sharding sharding) {
    ClusterLoadBanlance loadBanlance = loadBanlanceHolder.get();
    if (loadBanlance == null) {
      return DefaultLoadBanlance.getSingleton().getReadJedisPool(sharding);
    }
    return loadBanlance.getReadJedisPool(sharding);
  }

  public void setSlotState(int slot, SlotState slotState) {
    slotShardings.get(slot).setSlotState(slotState);
  }

  public void closeConnections(String nodeKey) {
    JedisPool jedisPool = nodes.get(nodeKey);
    if (jedisPool != null) {
      jedisPool.close();
    }
  }

  public static class Sharding {
    private volatile transient JedisPool master;
    private volatile transient List<JedisPool> slaves = new CopyOnWriteArrayList<JedisPool>();
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

    public void setSlaves(List<JedisPool> slaves) {
      this.slaves = slaves;
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
