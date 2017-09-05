/*
 * Copyright (c) Sebastian Ertel 2011. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality.handers;

import java.util.ArrayList;
import java.util.List;

import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OhuaOperator;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacket;

public class UserGraphEntranceEOSMarkerHandler extends EndOfStreamPacketHandler
{
  
  public UserGraphEntranceEOSMarkerHandler(OhuaOperator operator)
  {
    super(operator);
  }
  
  @Override
  public void notifyMarkerArrived(InputPort port, EndOfStreamPacket packet)
  {
    if(!packet.portsToClose().isEmpty())
    {
      handleBranchClose(packet);
    }
    else
    {
      super.notifyMarkerArrived(port, packet);
    }
  }
  
  private void handleBranchClose(EndOfStreamPacket packet)
  {
    List<PortID> portsToClose = new ArrayList<PortID>(packet.portsToClose());
    packet.portsToClose().clear();
    for(OutputPort outPort : _operator.getOutputPorts())
    {
      if(portsToClose.contains(outPort.getPortId()))
      {
        outPort.sendDataPacketNew(packet);
      }
    }
  }
}
