/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.util.List;
import java.util.Set;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;

public abstract class AbstractPortController implements OperatorStateAccess
{
  protected DataAccessLayer _dataAccess = null;
  protected PortID _portID = null;
  
  protected OhuaDataAccessor _accessor = null;
  
  private DataPacket _currentDataPacket = null;

  protected AbstractPortController(PortID portID, DataAccessLayer dataAccess)
  {
    _portID = portID;
    _dataAccess = dataAccess;
    _accessor = _dataAccess.getNewDataAccessor();
  }
  
  abstract public String getPortName();
  
  @Override
  public boolean equals(Object obj)
  {
    if(this == obj)
      return true;
    if((obj == null) || (obj.getClass() != this.getClass()))
      return false;

    return _portID.equals(((AbstractPortController) obj)._portID);
  }
  
  @Override
  public int hashCode()
  {
    return _portID.hashCode();
  }

  public String dataToString(String format)
  {
    return _accessor.dataToString(format);
  }

  protected void setCurrentDataPacket(DataPacket currentDataPacket)
  {
    _currentDataPacket = currentDataPacket;
    if(currentDataPacket != null)
    {
      _accessor.setDataRoot(currentDataPacket.getData());
    }
  }
  
  protected DataPacket getCurrentDataPacket()
  {
    return _currentDataPacket;
  }
  
  public Set<String> getLeafs()
  {
    return _accessor.getLeafs();
  }
  
  public List<Object> getData(String path)
  {
    return _accessor.getData(path);
  }
  
  public Object getData(){
    return _accessor.getDataRoot();
  }

  public void setData(String path, Object value) {
	  _accessor.setData(path, value);
  }

  @Override
  public Object getState() {
    // FIXME this needs deep copy as well!
    return _currentDataPacket;
  }

  @Override
  public void setState(Object state) {
    setCurrentDataPacket((DataPacket) state);
  }
}
