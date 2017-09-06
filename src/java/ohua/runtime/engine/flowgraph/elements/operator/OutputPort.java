/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.daapi.OutputPortController;
import ohua.runtime.engine.points.IOutputPortEventHandler;
import ohua.runtime.engine.points.OutputPortEvents;
import ohua.runtime.engine.flowgraph.elements.abstraction.GraphNodeOutput;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;

import java.util.*;

public class OutputPort extends AbstractPort implements GraphNodeOutput {
  private List<Arc> _outgoingArcs = new ArrayList<>();
  private List<IOutputPortEventHandler> _processingEventHandlers = new LinkedList<>();
  @SuppressWarnings("unchecked")
  private List<IOutputPortEventHandler>[] _finishEventHandlers = new List[OutputPortEvents.values().length];
  /**
   * This flag is very important as it is used to understand whether an operator finished its
   * computation or not.
   */
  private boolean _active = true;

  // idempotency. it can happen that an operator gets scheduled although it has not seen the
  // activation marker because it has been activated by an upstream operator (merge) in a data
  // processing phase. in that case we want to make sure that we do not send the EOS marker
  // downstream.
  private Class<? extends OutputPortController> _outControlType = OutputPortController.class;
  private ReturnStatus _returnValue = null;

  public OutputPort(OperatorCore op) {
    super(op);
    // there is no INIT state yet for output ports. in the INIT phase we do not allow any data
    // to get on the stream. the next phase will set the state of the output ports accordingly.
    setState(PortState.BLOCKED);
    init();
  }

  public OutputPort(OperatorCore op, String name) {
    this(op);
    setPortName(name);
  }


  public OutputPort(OperatorCore op, PortID portID) {
    super(op, portID);
    init();
  }

  public void initComplete(){
    super.initComplete();
    deactivate();
  }

  public Class<? extends OutputPortController> getOutputPortControllerType() {
    return _outControlType;
  }

  public void setOutputPortControllerType(Class<? extends OutputPortController> outControlType) {
    _outControlType = outControlType;
  }

  private void init() {
    _finishEventHandlers[OutputPortEvents.OUTPUT_PORT_CLOSED.ordinal()] = new LinkedList<>();
    _finishEventHandlers[OutputPortEvents.OUTPUT_PORT_FINISHED.ordinal()] = new LinkedList<>();
  }

  public void addArc(Arc a) {
    _outgoingArcs.add(a);
  }

  @Override
  public PortState getState() {
    return _state;
  }

  public boolean sendDataPacketNew(Object packet) {
    if (!_processingEventHandlers.isEmpty()) {
      boolean send = notifyHandlers(packet);
      if (send) {
        // execute the state
        sendDataPacket(packet);
      } else {
        // drop. send was already performed by the handlers (or swallowed)
      }
    } else {
      sendDataPacket(packet);
    }

    if (_returnValue == ReturnStatus.ALL_ARCS_BLOCKING) {
//      getOwner().notifyBackoffReason(BackoffReason.FULL_ARC);
      return false;
    } else {
      return true;
    }
  }

  /**
   * @param packet
   * @return false - do not enqueue any more data!
   */
  private void sendDataPacket(Object packet) {

    // in this function it should switch on its state!!!
    boolean suggestedBoundaryNotYetReached = true;
    boolean allArcsAreBlocking = true;
    boolean atLeastOnePortIsBlocking = false;

    for (Arc arc : _outgoingArcs) {
      suggestedBoundaryNotYetReached = arc.enqueue(packet);
      // getOwner().addDownstreamOpToBeActivated(arc.getTarget());

      // TODO: can we get this out of this loop somehow? not really because we do not know when
      // the operator stops producing new packets.
      // YES: to get something out of the way of the stream just always use a listener! here
      // just register a listener and after we have seen that we enqueued something into this
      // queue once just remove him out of this queue and the loop will be empty!
      // getOwner().activateOperator(arc.getTargetPort().getOwner());

      // ugly but I have no other solution for now
      if (!suggestedBoundaryNotYetReached) {

        if (!atLeastOnePortIsBlocking) {
          atLeastOnePortIsBlocking = true;
        }
      }

      if (allArcsAreBlocking && suggestedBoundaryNotYetReached) {
        allArcsAreBlocking = false;
      }
    }

    // do all the scheduling decisions in the scheduler by forcing this operator to give back
    // control. the operator might even get rescheduled right away when there are no other ops
    // in this section that are more downstream, but that is okay!
    if (allArcsAreBlocking) {
      _returnValue = ReturnStatus.ALL_ARCS_BLOCKING;
      return;
    }

    // FIXME this is really a problem. what if one of the downstream ops can continue processing
    // while another is waiting for other input from somewhere else?! in that case we will up
    // its arc and run out of memory!

    // we can not give back control here, because that would result in a deadlock situation!
    // hence the scheduling decision for the system level scheduler needs to be delayed until
    // all the arcs are filled up and we can give back control to the scheduler.
    if (atLeastOnePortIsBlocking) {
      _returnValue = ReturnStatus.AT_LEAST_ONE_ARC_BLOCKING;
      return;
    }

    // as long as there is at least one arc that is not blocking, we have to let the operator
    // produce more data, otherwise we might run into a deadlock situation -> triangle graph
    // structure
    _returnValue = ReturnStatus.NO_BLOCKING_ARCS;
  }

  public List<Arc> getOutgoingArcs() {
    return _outgoingArcs;
  }

  private boolean notifyHandlers(Object packet) {
    OutputPortEvents e = OutputPortEvents.EMIT_PACKET;
    e.setArgs(packet);
    notifyHandlers(_processingEventHandlers, e);
    return (Boolean) e.getResult();
  }

  private void notifyHandlers(List<IOutputPortEventHandler> handlers, OutputPortEvents event) {
    for (IOutputPortEventHandler handler : handlers) {
      handler.notifyOutputEvent(this, event);
    }
  }

  @Override
  public void close() {
    // idempotency
    if (_active) {
      // finish(true);
      notifyHandlers(_finishEventHandlers[OutputPortEvents.OUTPUT_PORT_CLOSED.ordinal()],
              OutputPortEvents.OUTPUT_PORT_CLOSED);
      deactivate();
    }
    // close the port
    setState(PortState.CLOSED);
  }

  protected void finish() {
    // idempotency
    if (_active) {
      // finish(false);
      notifyHandlers(_finishEventHandlers[OutputPortEvents.OUTPUT_PORT_FINISHED.ordinal()],
              OutputPortEvents.OUTPUT_PORT_FINISHED);
      deactivate();
    }

    setState(PortState.BLOCKED);
  }

  /**
   * This API only used by port handlers that deal with Fast Travelers. It is also used by the
   * network operators to propagate all data over the wire.
   *
   * @param toBePropagated
   */
  protected void enqueueMetaData(IMetaDataPacket toBePropagated) {
    for (Arc outArc : getOutgoingArcs()) {
      outArc.enqueueMetaData(toBePropagated);
//      getOwner().addDownstreamOpToBeActivated(outArc);
    }
  }

  // private void finish(boolean teardown)
  // {
  // // if(!_active)
  // // {
  // // return;
  // // }
  // //
  // // // send an end data packet to all succeeding ops
  // // for(Arc arc : _outgoingArcs)
  // // {
  // // // we do not serve feedback-edges here because those have been closed already by the
  // // // algorithm in the EOS handler!
  // // if(arc.getType() == ArcType.FEEDBACK_EDGE)
  // // {
  // // continue;
  // // }
  // //
  // // // // FIXME I think this wants to be done by the EOS handler!
  // // // arc.enqueue(PacketFactory.createEndSignalPacket(getOwner(), teardown));
  // // // _logger.log(Level.ALL, "end packet send from " + getOwner().getOperatorName() +
  // " to "
  // // // + arc.getTarget().getOperatorName());
  // // //
  // // // getOwner().addDownstreamOpToBeActivated(arc.getTarget());
  // // }
  // //
  // deactivate();
  // }

  public void activate() {
    _active = true;
  }

  public void deactivate() {
    _active = false;
  }

  public boolean isActive() {
    return _active;
  }

  public void registerEventListener(IOutputPortEventHandler outputPortEventHandler) {
    Set<OutputPortEvents> outEvents = outputPortEventHandler.getOutputPortEventInterest();
    for (final OutputPortEvents outEvent : outEvents) {
      List<IOutputPortEventHandler> handlers =
              outEvent == OutputPortEvents.EMIT_PACKET ? _processingEventHandlers : _finishEventHandlers[outEvent.ordinal()];
      handlers.add(outputPortEventHandler);

      Collections.sort(handlers, new Comparator<IOutputPortEventHandler>() {
        @Override
        public int compare(IOutputPortEventHandler o1, IOutputPortEventHandler o2) {
          if (o1.getPriority(outEvent) > o2.getPriority(outEvent)) return -1;
          return o1.getPriority(outEvent) < o2.getPriority(outEvent) ? 1 : 0;
        }
      });
    }
  }

  public String deadlockAnalysis() {
    StringBuffer analysis = new StringBuffer();
    analysis.append("OutputPort :" + this + "\n");
    analysis.append("OutputPort.PortState :" + getState() + "\n");
    analysis.append("OutputPort.isMetaPort :" + isMetaPort() + "\n");
    analysis.append("OutputPort.active :" + isActive() + "\n");
    getOutgoingArcs().stream().map(arc -> arc + "[boundary=" + arc.getArcBoundary() + "]\n").forEach(s -> analysis.append(s));
    return analysis.toString();
  }

  private enum ReturnStatus {
    ALL_ARCS_BLOCKING,
    AT_LEAST_ONE_ARC_BLOCKING,
    NO_BLOCKING_ARCS
  }

}
