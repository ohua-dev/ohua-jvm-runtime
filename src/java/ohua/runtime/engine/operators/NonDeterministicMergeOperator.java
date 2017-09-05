/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import ohua.runtime.engine.daapi.InputPortControl;

/**
 * This is the worst case merge that schedules after every poll operation!<br>
 * TODO: we need also a merge that schedules more realistically after having polled n packets
 * from ONE input port. In fact this can be a property of this merge!
 */
public class NonDeterministicMergeOperator extends AbstractMergeOperator
{
  public static class NonDeterministicMergeProperties extends MergeOperatorProperties implements Serializable
  {
    public int commitFrequency = 50;
  }
  
  protected int _packetsDequeuedFromCurrentPort = 0;
  
  public NonDeterministicMergeOperator() {
    setProperties(new NonDeterministicMergeProperties());
  }
  
  /**
   * We schedule all input ports in a round robin fashion and push the data to the output port.
   */
  @Override
  public void runProcessRoutine() {
    LinkedList<InputPortControl> currentOpenPorts = new LinkedList<InputPortControl>(getOpenInputPorts());
    while(!currentOpenPorts.isEmpty()) {
      if(isSwitchPorts()) {
        switchPorts(currentOpenPorts);
      }
      InputPortControl inPort = currentOpenPorts.getFirst();
      
      if(!inPort.next()) {
        if(inPort.hasSeenLastPacket()) {
          getOpenInputPorts().remove(inPort);
          currentOpenPorts.removeFirst();
        }
        else {
          switchPorts(currentOpenPorts);
          currentOpenPorts.removeLast();
        }
      }
      else {
        _packetsDequeuedFromCurrentPort++;
        if(emitOutput(inPort.getPortName())) {
          break;
        }
      }
    }
  }
  
  protected boolean emitOutput(String inputPort) {
    getDataLayer().transferInputToOutput(inputPort, _outPortControl.getPortName());
    return _outPortControl.send();
  }
  
  protected boolean isSwitchPorts() {
    return _packetsDequeuedFromCurrentPort == getProperties().getDequeueBatchSize();
  }
  
  private void switchPorts(LinkedList<InputPortControl> ports) {
    InputPortControl inPort = ports.removeFirst();
    ports.addLast(inPort);
    _packetsDequeuedFromCurrentPort = 0;
  }
  
  public Object getState() {
    Logger.getLogger("debug_cpInitiator").log(Level.ALL, "CHECKPOINT OF NDMergeOperator taken");
    return null;
  }

  @Override
  public void setState(Object state) {
    // stateless
  }
  
  public int getCommitFrequency() {
    return ((NonDeterministicMergeProperties) getProperties()).commitFrequency;
  }
  
}
