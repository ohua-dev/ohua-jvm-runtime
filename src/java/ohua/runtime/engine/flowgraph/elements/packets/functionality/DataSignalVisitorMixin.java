/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.daapi.DataPacket;
import ohua.runtime.engine.points.IDataPacketHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.util.Tuple;

/**
 * Mind that at that level we do not need access to the data directly but only to the meta
 * information of the packets!!!
 */
public class DataSignalVisitorMixin extends VisitorMixin<DataPacket, IDataPacketHandler>
{
  private Maybe _packetToReturn = Maybe.empty();
  
  public DataSignalVisitorMixin(InputPort in)
  {
    super(in);
  }
  
  @Override
  public Tuple<InputPort.VisitorReturnType, Maybe<Object>> handlePacket(DataPacket packet)
  {
    notifyHandlers(packet);
    return new Tuple<>(InputPort.VisitorReturnType.PACKET_WAS_HANDLED, Maybe.value(_packetToReturn, packet));
  }
  
  // TODO REFACTORING this loop should be generic in the super class and the function maybe does
  // need to be in the interface! --> problem: can not push the notifyMarkerArrived() function
  // into PacketHandlerInterface!
  public void notifyHandlers(DataPacket packet)
  {
    for(IDataPacketHandler handler : getAllHandlers())
      handler.notifyMarkerArrived(_inputPort, packet);
  }
  
  public InputPortEvents getEventResponsibility()
  {
    return InputPortEvents.DATA_PACKET_ARRIVAL;
  }
  
}
