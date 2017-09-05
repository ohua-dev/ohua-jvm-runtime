/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators.system;

import java.util.HashSet;
import java.util.Set;

import ohua.runtime.engine.points.IActivationMarkerHandler;
import ohua.runtime.engine.points.IEndOfStreamPacketHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.SystemOperator;
import ohua.runtime.engine.flowgraph.elements.packets.ActivationMarker;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacket;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.PriorityLevel;

/**
 * This operator will have (meta) input ports only and will be connected to all target operators
 * in the flow. It will be the final target of all meta data traveling along the stream (EOS
 * marker, End-Of-Graph-Analysis marker etc).<br>
 * The operator will be the last one to be scheduled in a system phase (initialization, graph
 * analysis, computation etc).
 * @author sertel
 * 
 */
public class UserGraphExitOperator extends SystemOperator implements
                                                         IEndOfStreamPacketHandler,
                                                         IActivationMarkerHandler,
                                                         OperatorStateAccess
{
  private Set<InputPort> _openInputPorts = new HashSet<>();
  
  @SuppressWarnings("unused") private ActivationMarker _systemPhaseMarker = null;

  @Override
  public void cleanup() {
    // no I/O here
  }
  
  @Override
  protected void prepareInputPorts() {
    super.prepareInputPorts();
    registerForEvents(InputPortEvents.ACTIVATION_PACKET_ARRIVAL);
    registerForEvents(InputPortEvents.END_OF_STREAM_PACKET_ARRIVAL);
  }
  
  @Override
  public void prepare() {
    // nothing
  }
  
  @Override
  public void runProcessRoutine() {
    // nothing to do here
  }
  
  public boolean systemPhaseCompleted() {
//    System.out.println("MetaTargetOperator has been asked for system phase completion and answered: " + _openInputPorts.isEmpty());
//    System.out.println("Number of ports still pending: " + _openInputPorts.size());
//    for(InputPort open : _openInputPorts){
//      System.out.println(open.getIncomingArc().getSource().getOperatorName());
//    }
    return _openInputPorts.isEmpty();
  }
  
  private void removeAndCheck(InputPort port) {
    _openInputPorts.remove(port);
  }
  
  public void addCallback(InputPortEvents event, InputPort port) {
    // nothing
  }
  
  public void init() {
    // nothing
  }
  
  public void restartInit() {
    // nothing
  }
  
  // FIXME there are two types here and we need to make sure that we saw both of the first type
  // and both of the second type.
  public void notifyMarkerArrived(InputPort port, EndOfStreamPacket packet) {
    removeAndCheck(port);
  }
  
  // TODO Put this back in once the cycle problem in the init phase is fixed.
  // @Override
  // protected boolean isTargetOperatorBlocking()
  // {
  // Assertion.invariant(_systemPhaseMarker != null);
  // return _systemPhaseMarker.isSystemPhase();
  // }
  
  public void notifyMarkerArrived(InputPort port, ActivationMarker packet) {
    // FIXME this is buggy. What happens if this operator gets executed but not yet find a
    // marker? it will just return with the empty set here and say that it is still done.
    // a section can get scheduled although it has no active operators by the section executor!
    // (Happens in the init phase when the merge runs on one section and returns, we still
    // activate the succeeding section which kind of ignites a chain reaction downstream to the
    // MetaTargetOperator.)
    _systemPhaseMarker = packet;
  }
  
  public void startNewSystemPhase() {
    _openInputPorts.addAll(getMetaInputPorts());
  }
    
  public Object getState() {
    return null;
  }
  
  public void setState(Object checkpoint) {
    startNewSystemPhase();
    prepareInputPorts();
  }
  
  public void removeCallback(InputPortEvents event, InputPort port) {
    throw new UnsupportedOperationException();
  }
  
  public PriorityLevel getPriority(InputPortEvents event) {
    return PriorityLevel.LOW;
  }
}
