/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.PortID.PortIDGenerator;

public abstract class AbstractPort
{
  // FIXME States that the operator core is setting due to a state transition should be hidden
  // from the user (at least on the overlay network level).
  public enum PortState
  {
    INIT,
    NORMAL,
    BLOCKED,
    CLOSED
  }
  
  private OperatorCore _owner = null;
  
  private PortID _portId = null;
  
  private String _portName = null;
  
  protected volatile PortState _state = PortState.INIT;
  protected PortState _previousState = PortState.NORMAL;
  
  private boolean _metaPort = false;
  
  public PortState getState()
  {
    return _state;
  }
  
  protected void setState(PortState state)
  {
    _state = state;
  }

  public String getPortName()
  {
    return _portName;
  }
  
  public void setPortName(String portName)
  {
    _portName = portName;
  }
  
  public AbstractPort(OperatorCore op)
  {
    this(op, PortIDGenerator.generateNewPortID());
  }
  
  public AbstractPort(OperatorCore op, PortID id)
  {
    _owner = op;
    _portId = id;
  }
  
  public OperatorCore getOwner()
  {
    return _owner;
  }
  
  public PortID getPortId()
  {
    return _portId;
  }
  
  @Override
  public String toString()
  {
    return "Port-" + _portId + "[" + _portName + "]";
  }
  
  @Override
  public boolean equals(Object obj)
  {
    if(this == obj)
    {
      return true;
    }
    if(obj == null || obj.getClass() != this.getClass())
    {
      return false;
    }
    
    // object must be AbstractPort at this point
    AbstractPort test = (AbstractPort) obj;
    boolean isEqual = _state == test.getState();
    isEqual &= _owner == test.getOwner();
    isEqual &= _portId == test.getPortId();
    
    return isEqual;
  }
  
  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 31 * hash + _portId.getIDInt();
    
    return hash;
  }
  
//  public final SystemPhaseType getProcessState()
//  {
//    return getOwner().getProcessState();
//  }
  
  public void setMetaPort(boolean metaPort)
  {
    _metaPort = metaPort;
  }
  
  public boolean isMetaPort()
  {
    return _metaPort;
  }
  
  public void initComplete() {
    setState(PortState.NORMAL);
  }
  
  public void block()
  {
    // idempotency needed otherwise we overwrite the state that we wanted to save!
    if(_state != PortState.BLOCKED)
    {
      _previousState = _state;
      setState(PortState.BLOCKED);
    }
  }
  
  public void unblock()
  {
    setState(_previousState);
  }
  
  public void close()
  {
    setState(PortState.CLOSED);
  }
  
  public void open()
  {
    Assertion.invariant(_state != PortState.INIT,
                        "Port " + this + " (owned by operator " + _owner
                            + ") was tried to open but was found in non-legal state: " + _state);
    setState(PortState.NORMAL);
  }

}
