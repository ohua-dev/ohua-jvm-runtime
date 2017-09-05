/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality.handers;

import java.util.LinkedList;
import java.util.List;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.IEndOfOperatorInitMarkerHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfOperatorInitMarker;

public class EndOfOperatorInitMarkerHandler implements IEndOfOperatorInitMarkerHandler
{
  private List<InputPort> _ports = new LinkedList<>();

  public void notifyMarkerArrived(InputPort port, EndOfOperatorInitMarker packet)
  {
    Assertion.invariant(_ports.contains(port));
    
    _ports.remove(port);
    
    port.initComplete();

    // FIXME this needs to go away as soon as EOS markers work already in the init phase!
    // REVIEW the should now do so. test and remove!
    port.setHasSeenLastPacket(true);

    if(_ports.isEmpty())
    {
      // FIXME this leads to an inconsistent state. it can now happen that the downstream op
      // that runs in its own thread already finishes initialization much earlier than this
      // operator actually finishes.
      sendInitPacketDownstream(port, packet);
    }

    // FIXME!
//    port.setCurrentPacketToBeReturned(Maybe.empty());
//    port.setVisitorReturnStatus(VisitorReturnType.PACKET_WAS_HANDLED);
  }
  
  public void addCallback(InputPortEvents event, InputPort port)
  {
    _ports.add(port);
  }
  
  public void init()
  {
    // nothing yet
  }
  
  public void restartInit()
  {
    // nothing yet
  }
  
  private void sendInitPacketDownstream(InputPort inPort, EndOfOperatorInitMarker packet)
  {
    OperatorCore op = inPort.getOwner();
    // FIXME refactoring
//    op.broadcast(packet);
    throw new UnsupportedOperationException("Dead code?!");
  }

  public void removeCallback(InputPortEvents event, InputPort port)
  {
    throw new UnsupportedOperationException();
  }

}
