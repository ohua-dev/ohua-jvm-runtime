/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality.handers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.IActivationMarkerHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.flowgraph.elements.operator.Arc.ArcType;
import ohua.runtime.engine.flowgraph.elements.packets.ActivationMarker;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.PriorityLevel;

public class ActivationMarkerHandler implements IActivationMarkerHandler
{
  private Set<InputPort> _inputPorts = new HashSet<>();
  private int _seen = 0;
  private OhuaOperator _op;

  public ActivationMarkerHandler(OhuaOperator op){
    _op = op;
  }
 
  public void notifyMarkerArrived(InputPort port, ActivationMarker packet)
  {
    _seen++;

    // already with the first marker we will send the activation packet downstream and the
    // operator can run.
    if(_seen == 1)
    {
      // the first activation marker to arrive needs to activate this operator again because
      // after this arrival we will hand down control to the (user) operator code and therefore
      // everything has to be ready even though we did not see all activation markers yet.
      performOperatorActivation(new ArrayList<>(_inputPorts), packet.getPhaseType(), _op);
      broadcast(port, packet);
    }
    
    // reset
    _seen = _seen % getNumInputPorts();
  }
  
  /**
   * Exclude feedback arcs.
   * 
   * @return
   */
  private int getNumInputPorts(){
    int count = 0;
    for(InputPort inPort : _inputPorts){
      count = inPort.getIncomingArc().getType() != ArcType.FEEDBACK_EDGE ? count + 1 : count;
    }
    return count;
  }
  
  /**
   * we deal with cycles here: just don't propagate down an output port with a feedback arc
   * @param port
   * @param packet
   */
  private void broadcast(InputPort port, ActivationMarker packet){
    for(OutputPort outPort : port.getOwner().getOutputPorts()){
      boolean send = true;
      for(Arc outArc : outPort.getOutgoingArcs()){
        // this assumes either all or nothing
        if(outArc.getType() == ArcType.FEEDBACK_EDGE){
          send = false;
          break;
        }
      }
      if(send) outPort.sendDataPacketNew(packet);
    }
  }
  
  
  public static void performOperatorActivation(List<InputPort> inPorts, SystemPhaseType t, OhuaOperator op)
  {
    Assertion.invariant(!inPorts.isEmpty());

    for(InputPort inPort : inPorts)
    {
      inPort.setHasSeenLastPacket(false);
    }
    
    for(OutputPort outPort : inPorts.get(0).getOwner().getOutputPorts())
    {
      if(t.isSystemPhase())
      {
        outPort.block();
      }
      else
      {
        outPort.open();
      }
      outPort.activate();
    }

    op.setProcessState(t);
  }
  
  public void addCallback(InputPortEvents event, InputPort port)
  {
    _inputPorts.add(port);
  }
  
  public void init()
  {
    // nothing
  }

  public void removeCallback(InputPortEvents event, InputPort port)
  {
    throw new UnsupportedOperationException();
  }
  
  public PriorityLevel getPriority(InputPortEvents event)
  {
    return PriorityLevel.FIRST;
  }
  
}
