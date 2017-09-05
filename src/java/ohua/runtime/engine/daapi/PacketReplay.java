/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.util.List;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;

public class PacketReplay implements PacketCursor, OperatorStateAccess
{
  @SuppressWarnings("unused")
  private PortID _port = null;

  private List<DataPacket> _packets = null;
  private int _replayIndex = 0;

  public PacketReplay(PortID port)
  {
    _port = port;
  }
  
  protected void setReplay(List<DataPacket> packets)
  {
    _packets = packets;
    _replayIndex = 0;
  }

  public DataPacket next()
  {
    if(_packets == null || _packets.size() - 1 < _replayIndex)
    {
      return null;
    }
    else
    {
      return _packets.get(_replayIndex++);
    }
  }
  
  @Override
  public Object getState() {
    return _replayIndex;
  }

  @Override
  public void setState(Object state) {
    _replayIndex = (int) state;
  }
  
}
