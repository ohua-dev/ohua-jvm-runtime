/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality.handers;

import ohua.runtime.engine.daapi.DataPacket;
import ohua.runtime.engine.points.IDataPacketHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;


public class DataPacketHandler implements IDataPacketHandler
{
//  private Maybe _packetToReturn = Maybe.empty();

  public void notifyMarkerArrived(InputPort port, DataPacket packet)
  {
//    port.setCurrentPacketToBeReturned(Maybe.value(_packetToReturn, packet));
//    port.setVisitorReturnStatus(VisitorReturnType.PACKET_WAS_HANDLED);
//    return new Tuple<>(VisitorReturnType.PACKET_WAS_HANDLED, Maybe.value(_packetToReturn, packet));
  }
  
  public void addCallback(InputPortEvents event, InputPort port)
  {
    // nothing to register here!
  }

  public void init()
  {
    // nothing for init here
  }

  public void restartInit()
  {
    // nothing here
  }
  
  public void removeCallback(InputPortEvents event, InputPort port)
  {
    // nothing here
  }
  
}
