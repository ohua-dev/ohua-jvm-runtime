/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import java.io.File;
import java.util.List;
import java.util.Set;

import ohua.runtime.engine.daapi.DataFormat;
import ohua.runtime.engine.daapi.DataPacket;
import ohua.runtime.engine.daapi.DataUtils;
import ohua.runtime.engine.daapi.OhuaDataAccessor;
import ohua.runtime.engine.points.InputPortEvents;

public class LanguageDataFormat implements DataFormat {
  
  public enum LanguagePacketAccess{
    MATCH
//    CONDITION
  }
  
  public static class LanguageDataPacket implements DataPacket {
    
    // FIXME does every packet need a fresh ds?! probably not! do on demand.
//    private Object[] _data = new Object[LanguagePacketAccess.values().length];
    private Object _data = null;
    
    @Override
    public InputPortEvents getEventType() {
      return InputPortEvents.DATA_PACKET_ARRIVAL;
    }
    
    @Override
    public DataPacket deepCopy() {
      LanguageDataPacket copy = new LanguageDataPacket();
//      copy._data = Arrays.copyOf(_data, _data.length);
      copy._data = _data;
      return copy;
    }
    
    @Override
    public String serialize() {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void deserialize(String data) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Object getData() {
      return _data;
    }
    
    @Override
    public void setData(Object dataRoot) {
      _data = dataRoot;
    }
  }
  
  public static class LanguageDataAccessor implements OhuaDataAccessor {
    private Object _currentDataRoot = null;
    
    @Override
    public void setResolutionRoot(String pathToResolutionRoot) {
      // there really isn't any hierarchical support here
    }
    
    @Override
    public void setDataRoot(Object resolutionRoot) {
      _currentDataRoot = resolutionRoot;
    }
    
    @Override
    public List<Object> getData(String path) {
      // TODO this is also overhead! we should refine the API here
      throw new UnsupportedOperationException();
//      return Collections.singletonList(_currentDataRoot);
    }
    
    @Override
    public void setData(String path, Object value) {
      // the assumption really is that the language layer just needs one spot per packet
      _currentDataRoot = value;
    }
    
    @Override
    public void load(File file) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Set<String> getLeafs() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public String dataToString(String format) {
      return _currentDataRoot.toString();
    }
    
    @Override
    public void parse(String data, String format) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Object getDataRoot() {
      return _currentDataRoot;
    }
    
    @Override
    public boolean elementExists(String string) {
      return true;
    }
  }
  
  public class LanguageDataUtils implements DataUtils
  {
    @Override
    public int compare(Object arg0, Object arg1)
    {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean equal(Object o1, Object o2)
    {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Object add(Object addend1, Object addend2)
    {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Object subtract(Object minuend, Object subtrahend)
    {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Object multiply(Object multiplier, Object multiplicant)
    {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Object divide(Object dividend, Object devisor)
    {
      throw new UnsupportedOperationException();
    }
  }

  
  @Override
  public DataPacket createDataPacket() {
    return new LanguageDataPacket();
  }
  
  @Override
  public OhuaDataAccessor createDataAccessor() {
    return new LanguageDataAccessor();
  }
  
  @Override
  public DataUtils getComparisonUtils() {
    return new LanguageDataUtils();
  }
  
}
