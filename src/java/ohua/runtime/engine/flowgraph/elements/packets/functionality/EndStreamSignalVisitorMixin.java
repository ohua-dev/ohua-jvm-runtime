/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.points.IEndOfStreamPacketHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacket;
import ohua.util.Tuple;

public class EndStreamSignalVisitorMixin extends
                                        VisitorMixin<EndOfStreamPacket, IEndOfStreamPacketHandler>
{
  public EndStreamSignalVisitorMixin(InputPort in)
  {
    super(in);
  }
  
  // TODO: A cleaner separation is needed! There should be only one guy that handles the packet,
  // but there can be multiple guys registered that receive a notification once one of those
  // packets arrives! (For now, I just make sure that really only one of the registered handlers
  // actually handles the packet!)
  @Override
  public Tuple<InputPort.VisitorReturnType, Maybe<Object>> handlePacket(EndOfStreamPacket packet) {
    notifyHandlers(packet);
    return new Tuple<>(InputPort.VisitorReturnType.PACKET_WAS_HANDLED, Maybe.empty());
  }
  
  /**
   * We notify the attached handlers. The "root" EOS packet handler will be notified last,
   * always!
   */
  public void notifyHandlers(EndOfStreamPacket packet)
  {
    List<IEndOfStreamPacketHandler> handlers =
        new ArrayList<>(getAllHandlers());
    // sorts in ascending order (that's why the comparator function is actually the other way
    // around than in the doc described!)
    Collections.sort(handlers,
                     new PriorityComparator(InputPortEvents.END_OF_STREAM_PACKET_ARRIVAL));
    for(IEndOfStreamPacketHandler handler : handlers) {
      handler.notifyMarkerArrived(_inputPort, packet);
    }
  }
  
  public InputPortEvents getEventResponsibility()
  {
    return InputPortEvents.END_OF_STREAM_PACKET_ARRIVAL;
  }
  
}
