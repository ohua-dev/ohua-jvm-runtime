/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;

public class PacketHeapSupport implements PacketCursor, OperatorStateAccess {
  @SuppressWarnings("unused")
  private PortID _port = null;
  
  private Map<String, List<DataPacket>> _heap = new HashMap<String, List<DataPacket>>();
  
  private PacketReplay _replayer = null;
  private String _currentReplayKey = null;
  
  public PacketHeapSupport(PortID portID) {
    _port = portID;
    _replayer = new PacketReplay(portID);
  }
  
  protected void store(String key, DataPacket packet) {
    if(!_heap.containsKey(key)) {
      _heap.put(key, new ArrayList<DataPacket>());
    }
    _heap.get(key).add(packet);
  }
  
  protected void query(String key) {
    _currentReplayKey = key;
    
    if(_heap.containsKey(key)) {
      _replayer.setReplay(_heap.get(key));
    } else {
      _replayer.setReplay(Collections.<DataPacket> emptyList());
    }
  }
  
  protected void clear() {
    _heap.clear();
  }
  
  @Override
  public Object getState() {
    // we need the deep copy here because it might happen that we not right away store the
    // checkpoint and we do not want to share references that make the checkpoint inconsistent.
    Map<String, List<DataPacket>> heapCopy = new HashMap<String, List<DataPacket>>(_heap.size());
    for(Map.Entry<String, List<DataPacket>> heapEntry : _heap.entrySet())
      heapCopy.put(heapEntry.getKey(), new ArrayList<DataPacket>(heapEntry.getValue()));
    return new Object[] { heapCopy,
                         _currentReplayKey,
                         _replayer.getState() };
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void setState(Object state) {
    Object[] s = (Object[]) state;
    _heap = (Map<String, List<DataPacket>>) s[0];
    
    _currentReplayKey = (String) s[1];
    query(_currentReplayKey);
    _replayer.setState(s[2]);
  }
    
  public DataPacket next() {
    return _replayer.next();
  }
}
