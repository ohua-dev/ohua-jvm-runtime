/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality.handers;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.IActivationMarkerHandler;
import ohua.runtime.engine.points.IEndOfStreamPacketHandler;
import ohua.runtime.engine.points.IOutputPortEventHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.points.OutputPortEvents;
import ohua.runtime.engine.points.PacketFactory;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.Arc.ArcType;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OhuaOperator;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;
import ohua.runtime.engine.flowgraph.elements.packets.ActivationMarker;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacket;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.PriorityLevel;
import ohua.runtime.engine.operators.system.ProcessControlOperator;

/**
 * This handler needs to be the last to be registered on every input port visitor!
 * <p>
 * In order to support cycles this concept needs to become a little bit more elaborate!
 * propagation rules:
 * <ol>
 * <li>All EOS markers get assigned an ID. The ID will be the level of the operator to close.
 * <li>The marker send downstream will have the level = me + 2 iff all input ports have seen an
 * EOS marker.
 * <li>The marker downstream will have the level = me iff all forward-edge input ports have seen
 * an EOS marker and there exists at least 1 feedback-edge input port.
 * <li>On arrival of an EOS marker the input port is closed iff the level is equal to the
 * operator's level.
 * </ol>
 */
public class EndOfStreamPacketHandler implements
                                     IEndOfStreamPacketHandler,
                                     IOutputPortEventHandler,
                                     IActivationMarkerHandler
{
  protected Logger _logger = Logger.getLogger(getClass().getCanonicalName());
  
  protected OhuaOperator _operator = null;
  private Set<InputPort> _servedInputPorts = new HashSet<InputPort>();
  
  protected SystemPhaseType _systemPhaseType = null;
  
  private int _expectedFeedbackMarkers = 0;
  
  // FIXME will go away when the init() function is only being called once for all the ports!
  private boolean _wasInitialized = false;
  
  private static final int CYCLE_SHUTDOWN = -100;
  
  public EndOfStreamPacketHandler(OhuaOperator operator) {
    if(operator == null) {
      throw new IllegalArgumentException();
    }
    
    _operator = operator;
  }
  
  public void addCallback(InputPortEvents event, InputPort port) {
    _servedInputPorts.add(port);
  }
  
  public void init() {
    if(_wasInitialized) {
      return;
    }

    // register for output port events
    for(OutputPort outPort : _operator.getOutputPorts()) {
      outPort.registerEventListener(this);
    }
    
    _wasInitialized = true;
  }
  
  /**
   * Currently we support cycles only inside the user flow graph, not in the system graph.
   * That's a todo for later and remains TBD whether special handling is even necessary there!
   */
  public void notifyMarkerArrived(InputPort port, EndOfStreamPacket packet) {
    Assertion.invariant(packet.getType() != null);
    
    if(packet.getLevelToClose() == CYCLE_SHUTDOWN) {
      switch(port.getIncomingArc().getType())
      {
        case CYCLE_START:
          // shut down this operator/port
          handlePortClose(port, packet);
          break;
        case FEEDBACK_EDGE:
          Assertion.impossible("This means that we never hit a cycle start. Hence we just hit the case where there was no associated cycle start in the graph.");
          break;
        case FORWARD_EDGE:
          // just broadcast the cycle shutdown EOS
          _operator.broadcast(packet, false);
          break;
      }
    }
    else {
      
      // FIXME refactoring needed: UserGraphEntranceEOSMarker handler and this class need to be
      // leaves!
      _systemPhaseType = packet.getType();
      int feedbackEdgeCount = getFeedBackEdgeCount();
      if(feedbackEdgeCount > 0) {
        if(port.getIncomingArc().getType() == ArcType.FEEDBACK_EDGE) {
          // received marker on feedback edge -> normal arrival (nothing to be propagated)
          _expectedFeedbackMarkers--;
        }
        else {
          // check whether we sent the feedback-EOS marker
          if(_expectedFeedbackMarkers > 0) {
            // normal arrival (nothing to be propagated)
          }
          else {
            _operator.broadcast(PacketFactory.createEndSignalPacket(CYCLE_SHUTDOWN), false);
            _expectedFeedbackMarkers = feedbackEdgeCount;
          }
        }
      }
      else {
        // normal arrival (nothing to be propagated)
      }
      
      // for sure we close that port here
      handlePortClose(port, packet);
    }
    
//    setPortPacketStatus(port);
  }
  
  private void handlePortClose(InputPort port, EndOfStreamPacket packet) {
    // it is really the last packet, so close this port
//    System.out.println("input port " + port.getPortId() + "[" + port + "] of operator "
//                             + port.getOwner().getOperatorName()
//                             + " has received the last packet and will be closed now!");
    // process signal
    port.setHasSeenLastPacket(true);
    
    if(_systemPhaseType == SystemPhaseType.TEARDOWN) {
      port.close();
    }
    else {
      // make sure we can process the activation marker in the next phase
      port.open();
    }
  }
  
//  protected void setPortPacketStatus(InputPort port) {
//    port.setCurrentPacketToBeReturned(Maybe.empty());
//    port.setVisitorReturnStatus(VisitorReturnType.PACKET_WAS_HANDLED);
//  }
  
  private int getFeedBackEdgeCount() {
    int count = 0;
    for(InputPort servedInputPort : _servedInputPorts) {
      if(servedInputPort.getIncomingArc().getType() == ArcType.FEEDBACK_EDGE) count++;
    }
    return count;
  }
  
  public void removeCallback(InputPortEvents event, InputPort port) {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Called during teardown.
   */
  public void notifyOutputEvent(OutputPort port, OutputPortEvents event) {
    // the process control operator never closes down it's output ports
    Assertion.invariant(!(_operator.getUserOperator() instanceof ProcessControlOperator));
    Assertion.invariant(_systemPhaseType != null, _operator.getUserOperator().getOperatorName());
    
    if(!containsCycleStarts(port)) {
      port.sendDataPacketNew(PacketFactory.createEndSignalPacket(port.getOwner(), _systemPhaseType));
    }
    else {
      // do not send anything down a cycle start arc
    }
  }
  
  private boolean containsCycleStarts(OutputPort port) {
    for(Arc outArc : port.getOutgoingArcs()) {
      if(outArc.getType() == ArcType.CYCLE_START) return true;
    }
    return false;
  }
  
  private String printDownstreamOps(List<Arc> outgoingArcs) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("[");
    for(Arc outgoingArc : outgoingArcs) {
      buffer.append(outgoingArc.getTarget().getOperatorName() + ", ");
    }
    buffer.append("]");
    return buffer.toString();
  }
  
  /**
   * We use the activation marker because in data phases the ProcessControl operator is not
   * given an EOS handler and the state of this marker is not being updated accordingly.
   * <p>
   * As a result: The state in the EOS handler that arrives at the UserGraphEntranceOperator can
   * not be trusted and we therefore correct it in the EOS handler of the UserGraphEntrance.
   * Hence all EOS markers inside the user graph carry the correct state!
   */
  public void notifyMarkerArrived(InputPort port, ActivationMarker packet) {
    _systemPhaseType = packet.getPhaseType();
  }
  
  public PriorityLevel getPriority(InputPortEvents event) {
    if(event instanceof InputPortEvents && ((InputPortEvents) event) == InputPortEvents.ACTIVATION_PACKET_ARRIVAL)
    {
      return PriorityLevel.LOW;
    }
    else {
      return PriorityLevel.FIRST;
    }
  }
  
  public Set<OutputPortEvents> getOutputPortEventInterest() {
    Set<OutputPortEvents> s = new HashSet<>();
    s.add(OutputPortEvents.OUTPUT_PORT_CLOSED);
    s.add(OutputPortEvents.OUTPUT_PORT_FINISHED);
    return s;
  }
  
  public int getPriority(OutputPortEvents event) {
    return 0;
  }
  
}
