package redis.clients.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import redis.clients.jedis.HostAndPort;
import redis.clients.util.ClusterNodeInformation.NodeFlag;

public class ClusterNodeInformationParser {
  private static final String SLOT_IMPORT_IDENTIFIER = "-<-";
  private static final String SLOT_MIGRATE_IDENTIFIER = "->-";
  private static final String SLOT_IN_TRANSITION_IDENTIFIER = "[";
  public static final int SLOT_INFORMATIONS_START_INDEX = 8;
  public static final int HOST_AND_PORT_INDEX = 1;

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

    String[] arrayHostAndPort = stringHostAndPort.split(":");
    return new HostAndPort(arrayHostAndPort[0].isEmpty() ? current.getHost() : arrayHostAndPort[0],
        arrayHostAndPort[1].isEmpty() ? current.getPort() : Integer.valueOf(arrayHostAndPort[1]));
  }

  // public static void fillSlotInformation(String[] slotInfoPartArray, ClusterNodeInformation info)
  // {
  // for (String slotRange : slotInfoPartArray) {
  // fillSlotInformationFromSlotRange(slotRange, info);
  // }
  // }

  // private static void fillSlotInformationFromSlotRange(String slotRange, ClusterNodeInformation
  // info) {
  // if (slotRange.startsWith(SLOT_IN_TRANSITION_IDENTIFIER)) {
  // // slot is in transition
  // int slot = Integer.parseInt(slotRange.substring(1).split("-")[0]);
  //
  // if (slotRange.contains(SLOT_IMPORT_IDENTIFIER)) {
  // // import
  // info.addSlotBeingImported(slot);
  // } else {
  // // migrate (->-)
  // info.addSlotBeingMigrated(slot);
  // }
  // } else if (slotRange.contains("-")) {
  // // slot range
  // String[] slotRangePart = slotRange.split("-");
  // for (int slot = Integer.valueOf(slotRangePart[0]); slot <= Integer.valueOf(slotRangePart[1]);
  // slot++) {
  // info.addAvailableSlot(slot);
  // }
  // } else {
  // // single slot
  // info.addAvailableSlot(Integer.valueOf(slotRange));
  // }
  // }

  public static List<Integer> getAvailableSlots(String[] slotRanges) {
    List<Integer> availableSlots = new ArrayList<Integer>();
    for (String slotRange : slotRanges) {
      if (slotRange.startsWith(SLOT_IN_TRANSITION_IDENTIFIER)) {
        continue;
      }
      if (slotRange.contains("-")) {
        // slot range
        String[] slotRangePart = slotRange.split("-");
        Integer end = Integer.valueOf(slotRangePart[1]);
        for (int slot = Integer.valueOf(slotRangePart[0]); slot <= end; slot++) {
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
