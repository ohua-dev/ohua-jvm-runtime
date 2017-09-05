/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.List;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.IPacketHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;

public abstract class SystemOperator extends AbstractOperatorAlgorithm
{
  private SystemOperatorAdapter _adapter = null;
  
  @Override
  protected final void setOperatorAlgorithmAdapter(OperatorAlgorithmAdapter adapter)
  {
    Assertion.invariant(adapter instanceof SystemOperatorAdapter);
    _adapter = (SystemOperatorAdapter) adapter;
    super.setOperatorAlgorithmAdapter(_adapter);
  }

  protected boolean isSourceOperatorWasLastPacket()
  {
    return true;
  }
  
  protected void prepareInputPorts()
  {
    // nothing by default
  }
  
  protected final SystemPhaseType getProcessState()
  {
    return _adapter.getProcessState();
  }
  
  protected final List<OutputPort> getOutputPortsImpl()
  {
    return _adapter.getOutputPortsImpl();
  }
  
  protected final List<InputPort> getMetaInputPorts()
  {
    return _adapter.getMetaInputPorts();
  }

  protected final boolean broadcast(IMetaDataPacket data)
  {
    return _adapter.broadcast(data);
  }

  protected void registerForEvents(InputPortEvents... events)
  {
    if(!(this instanceof IPacketHandler))
    {
      throw new UnsupportedOperationException("No Packet Handler interface implemented");
    }
    else
    {
      for(InputPort inPort : getMetaInputPorts())
      {
        for(InputPortEvents event : events)
        {
          inPort.registerForEvent(event, (IPacketHandler) this);
        }
      }
    }
  }
  
  protected void takeoverEvents(InputPortEvents... events)
  {
    if(!(this instanceof IPacketHandler))
    {
      throw new UnsupportedOperationException("No Packet Handler interface implemented");
    }
    else
    {
      for(InputPort inPort : getMetaInputPorts())
      {
        for(InputPortEvents event : events)
        {
          for(IPacketHandler handler : inPort.getHandlersForEvent(event))
          {
            inPort.unregisterFromEvent(event, handler);
          }
          inPort.registerForEvent(event, (IPacketHandler) this);
        }
      }
    }
  }
  
}
