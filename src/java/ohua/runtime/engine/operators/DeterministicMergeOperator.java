/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.exceptions.Assertion;

/**
 * ATTENTION: This operator does not work with cycles! It will end up in a deadlock state when
 * it is waiting for data from the loop-back arc when the data coming from there though is
 * depending on the data from the other arc (forward edge) of this operator.
 * @author sebastian
 * 
 */
public class DeterministicMergeOperator extends AbstractMergeOperator {

  private int _currentPortIndex = 0;
  private int _packetsDequeuedFromCurrentPort = 0;
  
  @SuppressWarnings("unused") private int _packetsDequeued = 0;
  
  @Override
  public void runProcessRoutine() {
    InputPortControl inPort = null;
    while(!getOpenInputPorts().isEmpty()) {
      Assertion.invariant(_currentPortIndex > -1);
      
      inPort = getOpenInputPorts().get(_currentPortIndex);
      // Assertion.invariant(inPort.getState() != PortState.CLOSED);
      
      if(!inPort.next()) {
        if(inPort.hasSeenLastPacket()) {
          InputPortControl old = inPort;
          inPort = switchPorts();
          
          // remove from open ports list
          getOpenInputPorts().remove(old);
          
          // get the new index
          _currentPortIndex = getOpenInputPorts().indexOf(inPort);
          continue;
        } else {
          // no data available on this port
          break;
        }
      }
      _packetsDequeued++;
      
      _packetsDequeuedFromCurrentPort++;
      // push the packet to the output port
      getDataLayer().transferInputToOutput(inPort.getPortName(), _outPortControl.getPortName());
      boolean backOff = _outPortControl.send();
      
      // switch the port
      if(_packetsDequeuedFromCurrentPort == getProperties().getDequeueBatchSize()) {
        switchPorts();
      }
      
      if(backOff) {
        break;
      }
    }
  }
  
  private InputPortControl switchPorts() {
    _currentPortIndex = (_currentPortIndex + 1) % getOpenInputPorts().size();
    Assertion.invariant(_currentPortIndex > -1);
    InputPortControl inPort = getOpenInputPorts().get(_currentPortIndex);
    _packetsDequeuedFromCurrentPort = 0;
    return inPort;
  }
  
  /*
   * checkpoint routines
   */
  public Object getState() {
    Logger.getLogger("debug_cpInitiator").log(Level.ALL, "CHECKPOINT OF DeterministicMerge taken");
    List<String> openPorts = new LinkedList<String>();
    for(InputPortControl inPort : getOpenInputPorts())
      openPorts.add(inPort.getPortName());
    return new Object[] { _currentPortIndex,
                         getProperties().getDequeueBatchSize(),
                         _packetsDequeuedFromCurrentPort,
                         openPorts };
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void setState(Object state) {
    Object[] s = (Object[]) state;
    
    _currentPortIndex = (int) s[0];
    getProperties().setDequeueBatchSize((int) s[1]);
    _packetsDequeuedFromCurrentPort = (int) s[2];
    
    getOpenInputPorts().clear();
    for(String port : (List<String>)s[3]) {
      getOpenInputPorts().add(getDataLayer().getInputPortController(port));
    }
    _outPortControl = getDataLayer().getOutputPortController("output");
  }
  
}
