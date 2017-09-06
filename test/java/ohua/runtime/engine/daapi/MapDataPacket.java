/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.daapi;

import ohua.runtime.engine.points.InputPortEvents;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MapDataPacket implements DataPacket, Serializable
{
  private Map<String, Object> _data = new HashMap<String, Object>();
  
  @Override
  public DataPacket deepCopy()
  {
    MapDataPacket clone = new MapDataPacket();
    clone.setData(new HashMap<String, Object>(_data));
    return clone;
  }
  
  @Override
  public String serialize()
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void deserialize(String data)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Map<String, Object> getData()
  {
    return _data;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void setData(Object dataRoot)
  {
    _data = (Map<String, Object>)dataRoot;
  }
  
  @Override public InputPortEvents getEventType() {
    return InputPortEvents.DATA_PACKET_ARRIVAL;
  }

}
