/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality;

import java.util.HashSet;
import java.util.Set;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.IPacketHandler;
import ohua.runtime.engine.points.IPacketVisitor;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.packets.IPacket;
import ohua.util.Tuple;

/**
 * Too bad, we can not make those mixins stateless!<br>
 * <p>
 * In the end there will be a visitor registered for every INPUT PORT of each operator. All
 * visitors of an operator will report to one handler, that cares about the actual functionality
 * and cares about the forwarding of the marker!
 * <p>
 * The mixin layer should decide what to do next, the notification is just for all guys that
 * need the information that a packet of a certain type has arrived!
 * @param <T>
 */
public abstract class VisitorMixin<T extends IPacket, S extends IPacketHandler> implements
                                                                                IPacketVisitor<S, T>
{
  protected InputPort _inputPort = null;
  private Set<S> _handlers = new HashSet<>();
  
  public VisitorMixin(InputPort in)
  {
    _inputPort = in;
  }
  
  public abstract Tuple<InputPort.VisitorReturnType, Maybe<Object>> handlePacket(T packet);
  
  public void registerMarkerHandler(S handler)
  {
    Assertion.invariant(handler != null);
    if(_handlers.contains(handler))
    {
      return;
    }
    
    _handlers.add(handler);
    handler.addCallback(getEventResponsibility(), _inputPort);
  }
  
  public void unregisterMarkerHandler(S handler)
  {
    handler.removeCallback(getEventResponsibility(), _inputPort);
    _handlers.remove(handler);
  }
  
  public Set<S> getAllHandlers()
  {
    return _handlers;
  }
  
}
