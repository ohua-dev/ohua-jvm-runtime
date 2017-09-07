/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.util.ArrayList;
import java.util.List;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;

public class PacketBufferSupport implements OperatorStateAccess, PacketBuffer, PacketCursor
{
  @SuppressWarnings("unused")
  private PortID _port = null;
  
  private List<DataPacket> _buffer = new ArrayList<>();
  private PacketReplay _replayer = null;
  
  protected PacketBufferSupport(PortID port) {
    _port = port;
    _replayer = new PacketReplay(port);
    // make the buffer shared memory
    _replayer.setReplay(_buffer);
  }
  
  @Override
  public Object getState() {
    return new Object[] { new ArrayList<DataPacket>(_buffer),
                         _replayer.getState() };
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void setState(Object state) {
    _buffer = (List<DataPacket>) ((Object[]) state)[0];
    _replayer.setReplay(_buffer);
    _replayer.setState(((Object[]) state)[1]);
  }
    
  public void buffer() {
    // nothing here because the item is already buffered
    System.out.println("WARNING: Request for buffering during replay of already buffered packets! Your operator algorithm might be wrong.");
  }
  
  public void buffer(DataPacket packet) {
    _buffer.add(packet);
  }
  
  public void clear() {
    _buffer.clear();
    _replayer.setReplay(_buffer);
  }
  
  public void replay() {
    // should never be called by the data access layer because this is the call that triggers
    // the switch from operator to packet buffer
    Assertion.impossible();
  }
  
  public void stop() {
    // should never be called because this call tells the data access layer to switch back to
    // the operator.
    Assertion.impossible();
  }
  
  public DataPacket next() {
    return _replayer.next();
  }
  
  public void reset() {
    _replayer.setReplay(_buffer);
  }
}
