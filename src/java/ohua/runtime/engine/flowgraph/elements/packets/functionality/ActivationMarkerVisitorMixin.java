/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.points.IActivationMarkerHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort.VisitorReturnType;
import ohua.runtime.engine.flowgraph.elements.packets.ActivationMarker;
import ohua.util.Tuple;

public class ActivationMarkerVisitorMixin extends
                                         VisitorMixin<ActivationMarker, IActivationMarkerHandler>
{
  
  public ActivationMarkerVisitorMixin(InputPort in)
  {
    super(in);
  }
  
  public void notifyHandlers(ActivationMarker packet)
  {
    List<IActivationMarkerHandler> handlers =
        new ArrayList<>(getAllHandlers());
    // sorts in ascending order (that's why the comparator function is actually the other way
    // around than in the doc described!)
    Collections.sort(handlers,
                     new PriorityComparator(InputPortEvents.ACTIVATION_PACKET_ARRIVAL));
    for(IActivationMarkerHandler handler : handlers)
    {
      handler.notifyMarkerArrived(_inputPort, packet);
    }
  }

  public InputPortEvents getEventResponsibility()
  {
    return InputPortEvents.ACTIVATION_PACKET_ARRIVAL;
  }
  
  @Override
  public Tuple<VisitorReturnType, Maybe<Object>> handlePacket(ActivationMarker packet)
  {
    notifyHandlers(packet);
    
//    _inputPort.setCurrentPacketToBeReturned(Maybe.empty());
//    _inputPort.setVisitorReturnStatus(VisitorReturnType.DEQUEUE_NEXT_PACKET);
    return new Tuple<>(VisitorReturnType.DEQUEUE_NEXT_PACKET, Maybe.empty());
  }
  
}
