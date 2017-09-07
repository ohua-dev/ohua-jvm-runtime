/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

public class SplitOperator extends UserOperator {

  public static OperatorDescription description(){
    OperatorDescription desc = new OperatorDescription();
    desc.setInputPorts(Collections.singletonList("input"));
    desc.setOutputPorts(Collections.emptyList());
    desc.setDynamicOutputPorts(true);
    return desc;
  }

  public static class SplitOperatorProperties implements Serializable {
    public int _schedulingInterval = 1;
    
    // TODO weight the outgoing arc. that way 20% can go that way and 80% can go that way. ->
    // could be very important for load balancing purposes.
    // private int _weight = 1;
    
    public void setSchedulingInterval(int schedulingInterval) {
      _schedulingInterval = schedulingInterval;
    }
    
    public int getSchedulingInterval() {
      return _schedulingInterval;
    }
  }
  
  private SplitOperatorProperties _properties = new SplitOperatorProperties();
  
  private int _currentPort = 0;
  private int _currentIntervalCounter = 0;
  
  private InputPortControl _inPortControl = null;
  private List<OutputPortControl> _outPortControls = new ArrayList<OutputPortControl>();
  
  @Override
  public void prepare() {
    _inPortControl = getDataLayer().getInputPortController("input");
    _outPortControls.clear();
    for(String outPort : getOutputPorts()) {
      _outPortControls.add(getDataLayer().getOutputPortController(outPort));
    }
  }
  
  @Override
  public void runProcessRoutine() {
    while(_inPortControl.next()) {
      // FIXME needs to be in a handler!
      // long start = System.nanoTime();
      
      OutputPortControl outPortControl = _outPortControls.get(_currentPort);
      getDataLayer().transferInputToOutput(_inPortControl.getPortName(), outPortControl.getPortName());
      boolean backOff = outPortControl.send();
      
      // System.out.println("{'Arrival':" + start
      // + ", 'Request': " + _inPortControl.getData("request-id").get(0)
      // + ", 'Operation': 'Split'"
      // + ", 'Processing': "+ (System.nanoTime() - start) + "}");
      
      _currentIntervalCounter++;
      _currentIntervalCounter = _currentIntervalCounter % _properties.getSchedulingInterval();
      if(_currentIntervalCounter == 0) {
        int previousPort = _currentPort;
        _currentPort = _currentPort + 1;
        _currentPort = _currentPort % _outPortControls.size();
      }
      
      if(backOff) {
        return;
      }
    }
  }
  
  @Override
  public void cleanup() {
    // nothing to clean up
  }
  
  public Object getState() {
    return new Object[] { _currentPort,
                         _currentIntervalCounter };
  }
  
  public void postCheckpoint() {
    // nothing
  }
  
  public void setState(Object state) {
    _currentPort = (Integer) ((Object[]) state)[0];
    _currentIntervalCounter = (Integer) ((Object[]) state)[1];
    prepare();
  }
  
  public SplitOperatorProperties getProperties() {
    return _properties;
  }
  
  public void setProperties(SplitOperatorProperties properties) {
    _properties = properties;
  }
  
}
