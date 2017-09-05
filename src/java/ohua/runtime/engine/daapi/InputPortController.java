/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.util.ArrayList;
import java.util.List;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;

class InputPortController extends AbstractPortController implements InputPortControl
{
  class InputPortPacketCursor implements PacketCursor
  {
    public DataPacket next()
    {
      Maybe<Object> maybe = _dataAccess.next(_inPort);
      // old semantics
      _lastDequeuedDataPacket = maybe.isPresent() ? (DataPacket) maybe.get() : null;
      return _lastDequeuedDataPacket;
    }
  }
  
  DataPacket _lastDequeuedDataPacket = null;
  
  private InputPortPacketCursor _portPacketCursor = null;
  PacketBufferSupport _bufferSupport = null;
  PacketHeapSupport _packetHeap = null;
  private PacketCursor _packetProvider = null;

  private InputPort _inPort = null;
  private ReplayMode _replayMode = null;
  
  protected InputPortController(PortID portID, DataAccessLayer dataAccess)
  {
    super(portID, dataAccess);
    _portPacketCursor = new InputPortPacketCursor();
    _inPort = dataAccess.getInputPort(portID);
    _bufferSupport = new PacketBufferSupport(portID);
    _packetHeap = new PacketHeapSupport(portID);
    _packetProvider = _portPacketCursor;
  }
  
  /**
   * If the EOS marker was received *and* we are not replaying anything.
   */
  public boolean hasSeenLastPacket()
  {
    return _inPort.hasSeenLastPacket();// && _packetProvider == _portPacketCursor;
  }

  public boolean next()
  {
    DataPacket packet = _packetProvider.next();
    setCurrentDataPacket(packet);
    return packet != null; // FIXME not a valid check (see comment in PacketProvider)
  }

  public boolean hasData(){
    return getCurrentDataPacket() != null || !_inPort.getIncomingArc().isQueueEmpty();
  }
  
  /*
   * Heap methods.
   */

  public void clean()
  {
    _packetHeap.clear();
  }
  
  public void query(String key)
  {
    _packetHeap.query(key);
  }
  
  public void store(String key)
  {
    _packetHeap.store(key, _lastDequeuedDataPacket);
  }

  /*
   * Buffering methods.
   */

  public void buffer()
  {
    _bufferSupport.buffer(_lastDequeuedDataPacket);
  }
  
  public void clear()
  {
    _bufferSupport.clear();
  }
  
  /**
   * Please note that the operation of setting the repeatedly the same ReplayMode is idempotent!
   */
  public void replay(ReplayMode mode)
  {
    _replayMode = mode;
    _packetProvider = mode.getReplayer(this);
  }
  
  public void stop()
  {
    _packetProvider = _portPacketCursor;
    setCurrentDataPacket(_lastDequeuedDataPacket);
  }
  
  public void reset()
  {
    _bufferSupport.reset();
  }
  
  @Override
  public Object getState() {
    List<Object> state = new ArrayList<>();
    state.add(super.getState());
    if(_packetProvider != _portPacketCursor) state.add(_replayMode);
    state.add(_lastDequeuedDataPacket);
    state.add(_bufferSupport.getState());
    state.add(_packetHeap.getState());
    return state;
  }

  @Override
  public void setState(Object state) {
    @SuppressWarnings("unchecked")
    List<Object> s = (List<Object>) state;
    super.setState(s.remove(0));
    if(s.get(0) instanceof ReplayMode) _replayMode = (ReplayMode) s.remove(0);
    _lastDequeuedDataPacket = (DataPacket) s.remove(0);
    _bufferSupport.setState(s.remove(0));
    _packetHeap.setState(s.remove(0));
  }

  @Override
  public String getPortName()
  {
    return _dataAccess.getInputPortName(_portID);
  }

}
