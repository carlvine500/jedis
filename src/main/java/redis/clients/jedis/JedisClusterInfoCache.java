package redis.clients.jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisClusterCommand.Operation;
import redis.clients.util.ClusterNodeInformation;
import redis.clients.util.ClusterNodeInformation.NodeFlag;
import redis.clients.util.ClusterNodeInformationParser;
import redis.clients.util.SafeEncoder;

public class JedisClusterInfoCache {
  public static final ClusterNodeInformationParser nodeInfoParser = new ClusterNodeInformationParser();

  private Map<String, JedisPool> nodes = new HashMap<String, JedisPool>();
  private Map<Integer, JedisPool> slots = new HashMap<Integer, JedisPool>();

  private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
  private final Lock r = rwl.readLock();
  private final Lock w = rwl.writeLock();
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
  }

  public void discoverClusterNodesAndSlots(Jedis jedis) {
	reloadSlotShardings(jedis);
    w.lock();

    try {
      this.nodes.clear();
      this.slots.clear();

      String localNodes = jedis.clusterNodes();
      for (String nodeInfo : localNodes.split("\n")) {
        ClusterNodeInformation clusterNodeInfo = nodeInfoParser.parse(nodeInfo, new HostAndPort(
            jedis.getClient().getHost(), jedis.getClient().getPort()));

        HostAndPort targetNode = clusterNodeInfo.getNode();
        setNodeIfNotExist(targetNode);
        assignSlotsToNode(clusterNodeInfo.getAvailableSlots(), targetNode);
      }
    } finally {
      w.unlock();
    }
  }

  public void discoverClusterSlots(Jedis jedis) {
    w.lock();

    try {
      this.slots.clear();

      List<Object> slots = jedis.clusterSlots();

      for (Object slotInfoObj : slots) {
        List<Object> slotInfo = (List<Object>) slotInfoObj;

        if (slotInfo.size() <= 2) {
          continue;
        }

        List<Integer> slotNums = getAssignedSlotArray(slotInfo);

        // hostInfos
        List<Object> hostInfos = (List<Object>) slotInfo.get(2);
        if (hostInfos.size() <= 0) {
          continue;
        }

        // at this time, we just use master, discard slave information
        HostAndPort targetNode = generateHostAndPort(hostInfos);

        setNodeIfNotExist(targetNode);
        assignSlotsToNode(slotNums, targetNode);
      }
    } finally {
      w.unlock();
    }
  }

  private HostAndPort generateHostAndPort(List<Object> hostInfos) {
    return new HostAndPort(SafeEncoder.encode((byte[]) hostInfos.get(0)),
        ((Long) hostInfos.get(1)).intValue());
  }
  
  public void setNodeIfNotExist(HostAndPort node){
	  setNodeIfNotExist(node,Operation.READWRITE);
  }
  
  private void setNodeIfNotExist(HostAndPort node,Operation op) {
    w.lock();
    try {
      String nodeKey = getNodeKey(node);
      if (nodes.containsKey(nodeKey)) return;

      JedisPool nodePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
          connectionTimeout, soTimeout, null, 0, null,op);
      nodes.put(nodeKey, nodePool);
    } finally {
      w.unlock();
    }
  }

  private void assignSlotToNode(int slot, HostAndPort targetNode) {
    w.lock();
    try {
      JedisPool targetPool = nodes.get(getNodeKey(targetNode));

      if (targetPool == null) {
        setNodeIfNotExist(targetNode);
        targetPool = nodes.get(getNodeKey(targetNode));
      }
      slots.put(slot, targetPool);
    } finally {
      w.unlock();
    }
  }

  private void assignSlotsToNode(List<Integer> targetSlots, HostAndPort targetNode) {
    w.lock();
    try {
      JedisPool targetPool = nodes.get(getNodeKey(targetNode));

      if (targetPool == null) {
        setNodeIfNotExist(targetNode);
        targetPool = nodes.get(getNodeKey(targetNode));
      }

      for (Integer slot : targetSlots) {
        slots.put(slot, targetPool);
      }
    } finally {
      w.unlock();
    }
  }

  public JedisPool getNode(String nodeKey) {
    r.lock();
    try {
      return nodes.get(nodeKey);
    } finally {
      r.unlock();
    }
  }

  public JedisPool getSlotPool(int slot) {
    r.lock();
    try {
      return slots.get(slot);
    } finally {
      r.unlock();
    }
  }

  public Map<String, JedisPool> getNodes() {
    r.lock();
    try {
      return new HashMap<String, JedisPool>(nodes);
    } finally {
      r.unlock();
    }
  }

  public static String getNodeKey(HostAndPort hnp) {
    return hnp.getHost() + ":" + hnp.getPort();
  }

  public static String getNodeKey(Client client) {
    return client.getHost() + ":" + client.getPort();
  }

  public static String getNodeKey(Jedis jedis) {
    return getNodeKey(jedis.getClient());
  }

  private List<Integer> getAssignedSlotArray(List<Object> slotInfo) {
    List<Integer> slotNums = new ArrayList<Integer>();
    for (int slot = ((Long) slotInfo.get(0)).intValue(); slot <= ((Long) slotInfo.get(1))
        .intValue(); slot++) {
      slotNums.add(slot);
    }
    return slotNums;
  }
  
	private volatile Map<Integer, Sharding> slotShardings;
	
	public void reloadSlotShardings(Jedis jedis) {
		String clusterNodes = jedis.clusterNodes();
		HostAndPort current = new HostAndPort(jedis.getClient().getHost(), jedis.getClient().getPort());
		
		reloadSlotShardings(clusterNodes, current);
	}

	public void reloadSlotShardings(String clusterNodes, HostAndPort current) {
		Map<Integer, Sharding> newSlotShardings = new HashMap<Integer, Sharding>(BinaryJedisCluster.HASHSLOTS, 1F);
		for (int i = 0; i < BinaryJedisCluster.HASHSLOTS; i++) {
			newSlotShardings.put(i, new Sharding());
		}
		List<ClusterNodeInformation> nodeInfos = new ArrayList<ClusterNodeInformation>();
		for (String nodeInfo : clusterNodes.split("\n")) {
			ClusterNodeInformation clusterNodeInfo = nodeInfoParser.parse(nodeInfo, current);
			nodeInfos.add(clusterNodeInfo);
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

		for (ClusterNodeInformation nodeInfo : nodeInfos) {
			List<Integer> availableSlots = nodeInfo.getAvailableSlots();
			ClusterNodeInformation masterNodeInfo = nodeInfo;
			if (nodeInfo.getFlags().contains(NodeFlag.MASTER)) {
				List<Integer> slotsBeingImported = masterNodeInfo.getSlotsBeingImported();
				for (Integer slot : slotsBeingImported) {
					newSlotShardings.get(slot).setSlotState(SlotState.IMPORTING);
				}
				List<Integer> slotsBeingMigrated = masterNodeInfo.getSlotsBeingMigrated();
				for (Integer slot : slotsBeingMigrated) {
					newSlotShardings.get(slot).setSlotState(SlotState.MIGRATING);
				}
			}

			if (availableSlots.isEmpty()) {
				masterNodeInfo = findMasterByNodeId(nodeInfos, nodeInfo.getSlaveOf());
				availableSlots = masterNodeInfo.getAvailableSlots();
			}
			for (Integer slot : availableSlots) {
				if (nodeInfo.getFlags().contains(NodeFlag.MASTER)) {
					newSlotShardings.get(slot).setMaster(nodes.get(getNodeKey(nodeInfo.getNode())));
				} else {
					newSlotShardings.get(slot).addSlave(nodes.get(getNodeKey(nodeInfo.getNode())));
				}
			}

		}
		slotShardings = newSlotShardings;
	}
	
	public JedisPool getMaster(int slot) {
		return slotShardings.get(slot).master;
	}

	public JedisPool getSlaveAtRandom(int slot) {
		List<JedisPool> list = slotShardings.get(slot).slaves;
		if (list.isEmpty()) {
			return getMaster(slot);
		}
		if (slotShardings.get(slot).isMoving()) {
			return getMaster(slot);
		}
		return list.get(randomNumber(list.size()));
	}
	
	public JedisPool getMasterOrSlaveAtRandom(int slot) {
		List<JedisPool> list = slotShardings.get(slot).slaves;
		if (list.isEmpty()) {
			return getMaster(slot);
		}
		int totalNode = list.size() + 1;
		int index = randomNumber(totalNode);
		if (index == list.size()) {
			return getMaster(slot);
		}
		if (slotShardings.get(slot).isMoving()) {
			return getMaster(slot);
		}
		return list.get(index);
	}
	
	private static int randomNumber(int size) {
		// random reference: http://www.oschina.net/question/157182_45274
		return (int) (Math.floor(Math.random() * size));
	}
	
	private ClusterNodeInformation findMasterByNodeId(List<ClusterNodeInformation> nodeInfos,
			String nodeId) {
		for (ClusterNodeInformation clusterNodeInformation : nodeInfos) {
			if (clusterNodeInformation.getNodeId().equals(nodeId)) {
				return clusterNodeInformation;
			}
		}
		return null;
	}
	
	public static class Sharding {
		private JedisPool master;
		private List<JedisPool> slaves = new ArrayList<JedisPool>(2);
		private volatile SlotState slotState = SlotState.STABLE;

		public boolean isMoving() {
			return slotState != SlotState.STABLE;
		}

		public JedisPool getMaster() {
			return master;
		}

		public void setMaster(JedisPool master) {
			this.master = master;
		}

		public void addSlave(JedisPool slave) {
			slaves.add(slave);
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
