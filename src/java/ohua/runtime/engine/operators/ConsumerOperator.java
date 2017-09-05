/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators;

import java.util.logging.Logger;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

public class ConsumerOperator extends UserOperator
{
  // parameters for testing/regression purposes
  private int _seenPackets = 0;
  private boolean _keepLastPacket = false;
  private String _lastPacket = null;
  
  private InputPortControl _inPortControl = null;
  
  public void keepLastPacket() {
    _keepLastPacket = true;
  }
  
  public int getSeenPackets() {
    return _seenPackets;
  }
  
  public String getLastPacket() {
    return _lastPacket;
  }
  
  @Override public void prepare() {
    _inPortControl = getDataLayer().getInputPortController("input");
  }
  
  @Override public void runProcessRoutine() {
    // int seenInThisRun = 0;
    while(_inPortControl.next()) {
      _seenPackets++;
      if(_keepLastPacket) _lastPacket = _inPortControl.dataToString("JSON");
      // seenInThisRun++;
    }
  }
  
  @Override public void cleanup() {
    Logger.getLogger(getClass().getCanonicalName() + "-result").info("Operator \"" + getOperatorName() + "\" has seen "
                                                             + _seenPackets + " packets.");
  }
    
  public Object getState() {
    return _seenPackets;
  }

  public void setState(Object state) {
    _seenPackets = (int) state;
    prepare();
  }

}
