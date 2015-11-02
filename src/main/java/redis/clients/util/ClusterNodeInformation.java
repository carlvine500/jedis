package redis.clients.util;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import redis.clients.jedis.HostAndPort;

public class ClusterNodeInformation {
  private HostAndPort node;
  private String[] slotRanges;
  private String nodeId;
  private String slaveOf;
  private EnumSet<NodeFlag> flags;

  public boolean isSameMaster(ClusterNodeInformation other) {
    if (other == null) {
      return false;
    }
    return this.slaveOf != null ? this.slaveOf.equals(other.slaveOf) : other.slaveOf == null;
  }

  public boolean isSameFlags(ClusterNodeInformation other) {
    return this.flags.equals(other);
  }

  // false:failover or masterChanged
  public boolean isSameSlot(ClusterNodeInformation other) {
    if (other == null) {
      return false;
    }
    return Arrays.equals(this.slotRanges, other.slotRanges);
  }

  @Override
  public int hashCode() {
    return nodeId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    ClusterNodeInformation other = (ClusterNodeInformation) obj;
    return this.nodeId.equals(other.nodeId);
  }

  public ClusterNodeInformation(HostAndPort node) {
    this.node = node;
    // this.availableSlots = new ArrayList<Integer>();
    // this.slotsBeingImported = new ArrayList<Integer>();
    // this.slotsBeingMigrated = new ArrayList<Integer>();
  }

  public String[] getSlotRanges() {
    return slotRanges;
  }

  public void setSlotRanges(String[] slotRanges) {
    this.slotRanges = slotRanges;
  }

  // public void addAvailableSlot(int slot) {
  // availableSlots.add(slot);
  // }
  //
  // public void addSlotBeingImported(int slot) {
  // slotsBeingImported.add(slot);
  // }
  //
  // public void addSlotBeingMigrated(int slot) {
  // slotsBeingMigrated.add(slot);
  // }

  public HostAndPort getNode() {
    return node;
  }

  public List<Integer> getAvailableSlots() {
    return ClusterNodeInformationParser.getAvailableSlots(slotRanges);
  }

  public List<Integer> getSlotsBeingImported() {
    return ClusterNodeInformationParser.getImportingSlots(slotRanges);
  }

  public List<Integer> getSlotsBeingMigrated() {
    return ClusterNodeInformationParser.getMigratingSlots(slotRanges);
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

    public static EnumSet<NodeFlag> parse(String nodeFlagsStr) {
      String[] flags = StringUtils.split(nodeFlagsStr, ',');
      EnumSet<NodeFlag> nodeFlags = EnumSet.noneOf(NodeFlag.class);
      for (String flag : flags) {
        nodeFlags.add(strNodeFlag.get(flag));
      }
      return nodeFlags;
    }

  }

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

  public void setFlags(EnumSet<NodeFlag> flags) {
    this.flags = flags;
  }

  // flags may be : slave,fail,noaddr
  public boolean isInactive() {
    return flags.contains(NodeFlag.NOFLAGS) //
        || flags.contains(NodeFlag.EVENTUAL_FAIL)//
        || flags.contains(NodeFlag.FAIL)//
        || flags.contains(NodeFlag.HANDSHAKE)//
        || flags.contains(NodeFlag.NOADDR)//
    ;
  }

  public boolean isActive() {
    return !isInactive();
  }

  public boolean isMaster() {
    return flags.contains(NodeFlag.MASTER); //
  }

  public boolean isSlave() {
    return flags.contains(NodeFlag.SLAVE); //
  }

}
