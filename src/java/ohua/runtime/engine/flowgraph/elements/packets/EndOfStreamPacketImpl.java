/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

import java.util.ArrayList;
import java.util.List;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;

public class EndOfStreamPacketImpl extends AbstractSignalPacket implements EndOfStreamPacket
{
  private int _levelToClose = -1;
  
  private SystemPhaseType _type = null;
  
  private List<PortID> _portsToClose = new ArrayList<PortID>();

  @SuppressWarnings("unused")
  private EndOfStreamPacketImpl()
  {
    // use the other constructor and provide me a level ID to be closed!
  }
  
  public EndOfStreamPacketImpl(int levelToClose, SystemPhaseType type)
  {
    if(type == null)
    {
      throw new IllegalArgumentException();
    }
    _levelToClose = levelToClose;
    _type = type;
  }

  public EndOfStreamPacketImpl(int levelToClose, SystemPhaseType type, List<PortID> portsToClose)
  {
    this(levelToClose, type);
    _portsToClose.addAll(portsToClose);
  }

  public int getLevelToClose()
  {
    return _levelToClose;
  }

  public IStreamPacket deepCopy()
  {
    // it's ok, this packet is never forwarded and the carried state can be shared between all
    // the directly succeeding operators
    return this;
  }
  
  public SystemPhaseType getType()
  {
    return _type;
  }
  
  public List<PortID> portsToClose()
  {
    return _portsToClose;
  }
  
  @Override public InputPortEvents getEventType() {
    return InputPortEvents.END_OF_STREAM_PACKET_ARRIVAL;
  }

}