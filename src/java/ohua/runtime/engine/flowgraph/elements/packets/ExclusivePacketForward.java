/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.packets;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OhuaOperator;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;

/**
 * The idea of the forward is that it does not work on the basis of marker ids but on the
 * arrival of the meta-data packets among the input ports of the operator. It is therefore
 * applicable exclusively to slow travelers.
 * 
 * @author sertel
 * 
 */
public class ExclusivePacketForward implements OperatorStateAccess
{
  private class IntegerReference implements Serializable
  {
    int i = 0;
  }
  
  // state
  private Map<PortID, IntegerReference> _arrivalMap = new HashMap<PortID, IntegerReference>();
  
  private OhuaOperator _operator = null;
  
  public ExclusivePacketForward(OhuaOperator operator)
  {
    _operator = operator;
    for(InputPort port : _operator.getInputPorts())
    {
      _arrivalMap.put(port.getPortId(), new IntegerReference());
    }
  }
  
  public void broadcast(InputPort port, IMetaDataPacket packet)
  {
    if(_arrivalMap.get(port.getPortId()).i == 0)
    {
      System.out.println("Forwarding marker: " + _operator.getUserOperator().getOperatorName());
      // forward
      _operator.broadcast(packet);
      
      // count all other input ports up (pending)
      for(IntegerReference pending : _arrivalMap.values())
      {
        pending.i++;
      }
      _arrivalMap.get(port.getPortId()).i = 0;
    }
    else
    {
      System.out.println("Dropping marker: " + _operator.getUserOperator().getOperatorName());
      // count this port down (one less pending)
      _arrivalMap.get(port.getPortId()).i--;
    }
  }
  
  @Override
  public Object getState()
  {
    return _arrivalMap;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void setState(Object checkpoint)
  {
    _arrivalMap = (Map<PortID, IntegerReference>) checkpoint;
  }
}
