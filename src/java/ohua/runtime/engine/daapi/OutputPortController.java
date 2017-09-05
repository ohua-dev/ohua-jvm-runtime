/*
 * Copyright � Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.io.File;

import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;

public class OutputPortController extends AbstractPortController implements OutputPortControl {
  private DataPacket _template = null;
  
  // TBD: this is state and needs to be updated once we change ports online but maybe we just
  // want to create a new OutputPortController in that case!
  
  // possible solution: use a function pointer (anon class or lambda) here instead
  protected OutputPort _port = null;
  
  public OutputPortController(PortID portID, DataAccessLayer dataAccess) {
    super(portID, dataAccess);
    _template = _dataAccess.newEmptyDataPacket();
    _port = _dataAccess.getOutputPort(portID);
  }
  
  public boolean send() {
    assert getCurrentDataPacket() != null;
    getCurrentDataPacket().setData(_accessor.getDataRoot());
    boolean returnValue = _dataAccess.send(_port, getCurrentDataPacket());
    setCurrentDataPacket(null);
    return returnValue;
  }
  
  public void newPacket() {
    DataPacket packet = _template.deepCopy();
    _accessor.setDataRoot(packet.getData());
    setCurrentDataPacket(packet);
  }
  
  public void load(File file) {
    _accessor.load(file);
    _template = _dataAccess.newEmptyDataPacket();
    _template.setData(_accessor.getDataRoot());
  }
  
  @Override
  public void parse(String data, String format) {
    _accessor.parse(data, format);
  }
  
  public void setData(String path, Object value) {
    _accessor.setData(path, value);
  }
  
  @Override
  public String getPortName() {
    return _dataAccess.getOutputPortName(_portID);
  }
  
}
