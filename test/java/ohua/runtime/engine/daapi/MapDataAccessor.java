/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.daapi;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

public class MapDataAccessor implements OhuaDataAccessor
{
  private Map<String, Object> _currentDataRoot = new HashMap<>();
  
  @Override
  public void setResolutionRoot(String pathToResolutionRoot) {
    // there really isn't any hierarchical support here
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void setDataRoot(Object resolutionRoot)
  {
    _currentDataRoot = (Map<String, Object>) resolutionRoot;
  }
  
  @Override
  public List<Object> getData(String path)
  {
    return Collections.singletonList(_currentDataRoot.get(path));
  }
  
  @Override
  public void setData(String path, Object value)
  {
    _currentDataRoot.put(path, value);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void load(File file) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Set<String> getLeafs() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override
  public String dataToString(String format)
  {
    return _currentDataRoot.toString();
  }
  
  @Override
  public void parse(String data, String format) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Object getDataRoot()
  {
    // TODO Auto-generated method stub
    return _currentDataRoot;
  }
  
  @Override
  public boolean elementExists(String string)
  {
    return _currentDataRoot.containsKey(string);
  }
  
}
