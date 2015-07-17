package redis.clients.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.HostAndPort;

public class ClusterNodeInformation {
  private HostAndPort node;
  private List<Integer> availableSlots;
  private List<Integer> slotsBeingImported;
  private List<Integer> slotsBeingMigrated;

  public ClusterNodeInformation(HostAndPort node) {
    this.node = node;
    this.availableSlots = new ArrayList<Integer>();
    this.slotsBeingImported = new ArrayList<Integer>();
    this.slotsBeingMigrated = new ArrayList<Integer>();
  }

  public void addAvailableSlot(int slot) {
    availableSlots.add(slot);
  }

  public void addSlotBeingImported(int slot) {
    slotsBeingImported.add(slot);
  }

  public void addSlotBeingMigrated(int slot) {
    slotsBeingMigrated.add(slot);
  }

  public HostAndPort getNode() {
    return node;
  }

  public List<Integer> getAvailableSlots() {
    return availableSlots;
  }

  public List<Integer> getSlotsBeingImported() {
    return slotsBeingImported;
  }

  public List<Integer> getSlotsBeingMigrated() {
    return slotsBeingMigrated;
  }

  public enum NodeFlag {
    NOFLAGS("noflags"), MYSELF("myself"), SLAVE("slave"), MASTER("master") //
    , EVENTUAL_FAIL("fail?"), FAIL("fail"), HANDSHAKE("handshake"), NOADDR("noaddr");
    private static final Map<String, NodeFlag> strNodeFlag = new HashMap<String, NodeFlag>();
    static {
      for (NodeFlag nodeFlag : NodeFlag.values()) {
        strNodeFlag.put(nodeFlag.getNodeFlagString(), nodeFlag);
      }
    }

    private NodeFlag() {
    }

    private String nodeFlagString;

    private NodeFlag(String nodeFlagString) {
      this.nodeFlagString = nodeFlagString;
    }

    public String getNodeFlagString() {
      return nodeFlagString;
    }

    public static Set<NodeFlag> parse(String nodeFlagsStr) {
      Set<NodeFlag> set = new HashSet<ClusterNodeInformation.NodeFlag>();
      String[] flags = nodeFlagsStr.split(",");
      for (String flag : flags) {
        set.add(strNodeFlag.get(flag));
      }
      return set;
    }
  }

  private String nodeId;
  private String slaveOf;
  private Set<NodeFlag> flags;

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public String getSlaveOf() {
    return slaveOf;
  }

  public void setSlaveOf(String slaveOf) {
    this.slaveOf = slaveOf;
  }

  public Set<NodeFlag> getFlags() {
    return flags;
  }

  public void setFlags(Set<NodeFlag> flags) {
    this.flags = flags;
  }

  public boolean isDead() {
    return flags.contains(NodeFlag.EVENTUAL_FAIL) //
        || flags.contains(NodeFlag.FAIL) //
        || flags.contains(NodeFlag.HANDSHAKE) //
        || flags.contains(NodeFlag.NOADDR);
  }

  public boolean isAlive() {
    return !isDead();
  }

}
