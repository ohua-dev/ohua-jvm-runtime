/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

import java.util.List;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.daapi.DataPacket;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;
import ohua.runtime.engine.flowgraph.elements.packets.ActivationMarker;
import ohua.runtime.engine.flowgraph.elements.packets.ActivationMarkerImpl;
import ohua.runtime.engine.flowgraph.elements.packets.DataPollOnBlockedInputPortEvent;
import ohua.runtime.engine.flowgraph.elements.packets.DataPollOnBlockedInputPortSignal;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacket;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacketImpl;

public class PacketFactory
{
  public static DataPacket createDataPacket(DataPacket packet) {
    return packet;
  }
  
  public static EndOfStreamPacket createEndSignalPacket(OperatorCore operator, SystemPhaseType type) {
    return createEndSignalPacket(operator.getLevel() + 2, type);
  }
  
  public static EndOfStreamPacket createEndSignalPacket(int levelToClose, SystemPhaseType type) {
    return new EndOfStreamPacketImpl(levelToClose, type);
  }
  
  public static EndOfStreamPacket createConditionalEndSignalPacket(int levelToClose, SystemPhaseType type,
                                                                   List<PortID> eosNeeded)
  {
    return new EndOfStreamPacketImpl(levelToClose, type, eosNeeded);
  }
  
  public static EndOfStreamPacket createEndSignalPacket(int levelToClose) {
    return createEndSignalPacket(levelToClose, SystemPhaseType.COMPUTATION);
  }
  
  public static DataPollOnBlockedInputPortEvent createDataPollOnBlockedInputPortEvent() {
    return new DataPollOnBlockedInputPortSignal();
  }
  
  public static ActivationMarker createActivationMarkerPacket(SystemPhaseType phaseType) {
    return new ActivationMarkerImpl(phaseType);
  }
}
