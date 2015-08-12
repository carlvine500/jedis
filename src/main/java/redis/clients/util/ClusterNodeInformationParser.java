package redis.clients.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.HostAndPort;
import redis.clients.util.ClusterNodeInformation.NodeFlag;

public class ClusterNodeInformationParser {
  private static final String SLOT_IMPORT_IDENTIFIER = "-<-";
  private static final String SLOT_MIGRATE_IDENTIFIER = "->-";
  private static final String SLOT_IN_TRANSITION_IDENTIFIER = "[";
  public static final int SLOT_INFORMATIONS_START_INDEX = 8;
  public static final int HOST_AND_PORT_INDEX = 1;

  public static Map<String, List<String>> getMasterSlaveNodeIds(
      Map<String, ClusterNodeInformation> nodeInfos) {
    Map<String, List<String>> masterSlaves = new HashMap<String, List<String>>();
    for (ClusterNodeInformation nodeInfo : nodeInfos.values()) {
      String masterNodeId = null;
      if (nodeInfo.isSlave()) {
        masterNodeId = nodeInfos.get(nodeInfo.getSlaveOf()).getNodeId();
      } else if (nodeInfo.isMaster()) {
        masterNodeId = nodeInfo.getNodeId();
      } else {
        continue;
      }
      List<String> slaveNodeIds = masterSlaves.get(masterNodeId);
      if (slaveNodeIds == null) {
        slaveNodeIds = new ArrayList<String>();
      }
      if (nodeInfo.isSlave()) {
        slaveNodeIds.add(nodeInfo.getNodeId());
      }
      masterSlaves.put(masterNodeId, slaveNodeIds);
    }
    return masterSlaves;
  }

  public static Map<String, ClusterNodeInformation> parseAll(String clusterNodes,
      HostAndPort current) {
    String[] nodeInfos = clusterNodes.split("\n");
    Map<String, ClusterNodeInformation> nodeInfoMap = new HashMap<String, ClusterNodeInformation>(
        nodeInfos.length, 1F);
    for (String nodeInfo : nodeInfos) {
      ClusterNodeInformation clusterNodeInfo = ClusterNodeInformationParser
          .parse(nodeInfo, current);
      nodeInfoMap.put(clusterNodeInfo.getNodeId(), clusterNodeInfo);
    }
    return nodeInfoMap;
  }

  public static ClusterNodeInformation parse(String nodeInfo, HostAndPort current) {
    String[] nodeInfoPartArray = nodeInfo.split(" ");

    HostAndPort node = getHostAndPortFromNodeLine(nodeInfoPartArray, current);
    ClusterNodeInformation info = new ClusterNodeInformation(node);

    if (nodeInfoPartArray.length >= SLOT_INFORMATIONS_START_INDEX) {
      String[] slotInfoPartArray = extractSlotParts(nodeInfoPartArray);
      info.setSlotRanges(slotInfoPartArray);
      // fillSlotInformation(slotInfoPartArray, info);
    }

    info.setNodeId(nodeInfoPartArray[0]);

    String flagString = nodeInfoPartArray[2];
    info.setFlags(NodeFlag.parse(flagString));

    String slaveOf = nodeInfoPartArray[3];
    if (!slaveOf.equals("-")) {
      info.setSlaveOf(slaveOf);
    }

    return info;
  }

  private static String[] extractSlotParts(String[] nodeInfoPartArray) {
    String[] slotInfoPartArray = new String[nodeInfoPartArray.length
        - SLOT_INFORMATIONS_START_INDEX];
    for (int i = SLOT_INFORMATIONS_START_INDEX; i < nodeInfoPartArray.length; i++) {
      slotInfoPartArray[i - SLOT_INFORMATIONS_START_INDEX] = nodeInfoPartArray[i];
    }
    return slotInfoPartArray;
  }

  /**
   * be care, sometimes the output likes this: fd80d1696a8af7c6148db3a824dadbb09622227a :8000
   * myself,master - 0 0 0 connected 0-16300
   */
  public static HostAndPort getHostAndPortFromNodeLine(String[] nodeInfoPartArray,
      HostAndPort current) {
    String stringHostAndPort = nodeInfoPartArray[HOST_AND_PORT_INDEX];
    int indexOf = stringHostAndPort.indexOf(':');
    String host = stringHostAndPort.substring(0, indexOf);
    String port = stringHostAndPort.substring(indexOf + 1);
    return new HostAndPort(host.isEmpty() ? current.getHost() : host,
        port.isEmpty() ? current.getPort() : Integer.valueOf(port));
  }

  public static List<Integer> getAvailableSlots(String[] slotRanges) {
    List<Integer> availableSlots = Collections.emptyList();
    for (String slotRange : slotRanges) {
      if (slotRange.startsWith(SLOT_IN_TRANSITION_IDENTIFIER)) {
        continue;
      }
      if (slotRange.contains("-")) {
        // slot range
        String[] slotRangePart = slotRange.split("-");
        Integer end = Integer.valueOf(slotRangePart[1]);
        for (int slot = Integer.valueOf(slotRangePart[0]); slot <= end; slot++) {
          if (availableSlots.isEmpty()) {
            availableSlots = new ArrayList<Integer>();
          }
          availableSlots.add(slot);
        }
      } else {
        // single slot
        availableSlots.add(Integer.valueOf(slotRange));
      }
    }
    return availableSlots;
  }

  public static List<Integer> getImportingSlots(String[] slotRanges) {
    List<Integer> slots = Collections.emptyList();
    for (String slotRange : slotRanges) {
      if (!slotRange.startsWith(SLOT_IN_TRANSITION_IDENTIFIER)) {
        continue;
      }
      if (slotRange.contains(SLOT_IMPORT_IDENTIFIER)) {
        if (slots.isEmpty()) {
          slots = new ArrayList<Integer>();
        }
        int slot = Integer.parseInt(slotRange.substring(1, slotRange.indexOf('-')));
        slots.add(slot);
      }
    }
    return slots;
  }

  public static List<Integer> getMigratingSlots(String[] slotRanges) {
    List<Integer> slots = Collections.emptyList();
    for (String slotRange : slotRanges) {
      if (!slotRange.startsWith(SLOT_IN_TRANSITION_IDENTIFIER)) {
        continue;
      }
      if (slotRange.contains(SLOT_MIGRATE_IDENTIFIER)) {
        if (slots.isEmpty()) {
          slots = new ArrayList<Integer>();
        }
        int slot = Integer.parseInt(slotRange.substring(1, slotRange.indexOf('-')));
        slots.add(slot);
      }
    }
    return slots;
  }

}
