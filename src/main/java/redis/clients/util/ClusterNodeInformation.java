package redis.clients.util;

import java.util.ArrayList;
import java.util.List;
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
		NOFLAGS, MYSELF, SLAVE, MASTER, EVENTUAL_FAIL, FAIL, HANDSHAKE, NOADDR;
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
	
}
