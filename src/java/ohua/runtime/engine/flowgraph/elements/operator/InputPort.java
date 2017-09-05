/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.daapi.DataPacket;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.IPacketHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.abstraction.GraphNodeInput;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.flowgraph.elements.packets.IPacket;
import ohua.runtime.engine.flowgraph.elements.packets.ISpecialMetaDataPacket;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.VisitorMixin;
import ohua.util.Tuple;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static ohua.runtime.engine.flowgraph.elements.operator.InputPort.VisitorReturnType.PACKET_WAS_HANDLED;

@SuppressWarnings({"unchecked",
        "rawtypes"})
// -> visitor
public class InputPort extends AbstractPort implements GraphNodeInput {
  private final VisitorMixin[] _packetVisitors = new VisitorMixin[InputPortEvents.values().length];
  private AtomicReference<Arc> _incomingArc = null;
//  private Maybe<Object> _currentPacketToBeReturned = null;
//  private VisitorReturnType _visitorReturnStatus = VisitorReturnType.NOT_MY_BUISNESS;
  private boolean _lastPacketReceived = false;
  // for debugging purposes
//  private Object _dequeuedPacket = null;
  @SuppressWarnings("unused")
//  private VisitorMixin _hasHandledLastPacket = null;
  /**
   * This flag can be enabled to say the data arriving on this port is complimentary to the data
   * in the flow. Hence when this flag is disabled then the flow control will not ask this port
   * when making control decisions such as shutting down the flow or closing out on a
   * computation cycle.
   */
  private boolean _upstreamControlled = true;

  public InputPort(OperatorCore op) {
    super(op);
  }

  public InputPort(OperatorCore op, PortID portID) {
    super(op, portID);
  }

  public void initComplete(){
    super.initComplete();
    setHasSeenLastPacket(true);
  }

  public Maybe<Object> dequeueDataPacket() {
    boolean dequeueNextPacket = true;
    Maybe<Object> packet = null;
    while (dequeueNextPacket) {
      if (_state == PortState.BLOCKED) {
//        packet = PacketFactory.createDataPollOnBlockedInputPortEvent();
      } else {
        packet = _incomingArc.get().getData();
//        _dequeuedPacket = packet;
        if (!packet.isPresent()) {
          return packet;
        } else {
          Tuple<VisitorReturnType, Maybe<Object>> r = notifyPacketListeners(packet);
          packet = r._t;
          dequeueNextPacket = handleVisitorReturnStatus(r._s, r._t);
        }
      }
    }

    // if this is a meta port then the input must come from a meta operator and the scheduling
    // of those operators is handled elsewhere.
    // if(!isMetaPort())
    // // if(!_lastPacketReceived && !isMetaPort())
    // {
    // getOwner().addUpstreamOpToBeActivated(this);
    // // getOwner().addUpstreamOpToBeActivated(getIncomingArc().getSource());
    // }
    Assertion.invariant(packet != null);
    return packet;
  }

//  private Tuple<VisitorReturnType, Maybe<Object>> _p = new Tuple<>(PACKET_WAS_HANDLED, null);
  private Tuple<VisitorReturnType, Maybe<Object>> notifyPacketListeners(Maybe<Object> packet) {
    // Assertion.invariant(!_packetVisitorsMap.isPresent());
    Assertion.invariant(packet.isPresent());

    // TODO: currently you can not have packet notifications that are no packets but raw data. it makes sense to move
    //       routines in DataPacket to a stand-alone class -> abandon the inheritance.
    // FIXME I could not understand whether this check is the expensive thing!
    if (isDataPacket(packet) || !hasInterest(packet)) {
      // FIXME probably setting these two things makes all the difference! probably need to rewrite the API here.
      // fast path for data packets
//      setCurrentPacketToBeReturned(packet);
//      setVisitorReturnStatus(VisitorReturnType.PACKET_WAS_HANDLED);
//      _p._t = packet;
//      return _p;
      return new Tuple<>(PACKET_WAS_HANDLED, packet);
    } else {
//      _visitorReturnStatus = VisitorReturnType.NOT_MY_BUISNESS;

      IPacket p = (IPacket) packet.get();
      InputPortEvents event = p.getEventType();
      VisitorMixin visitor = _packetVisitors[event.ordinal()];

      try {
        return visitor.handlePacket(p);
      } catch (NullPointerException npe) {
        if (visitor == null) throw new RuntimeException("No visitor for event '" + event + "' found at operator '"
                + getOwner().getOperatorName() + "'!");
        else throw npe;
      }

      // this says that there is only one visitor that handles the packet correctly! (which is
      // correct!)
//      if (_visitorReturnStatus != NOT_MY_BUISNESS) {
//        _hasHandledLastPacket = visitor;
//        return true;
//      } else {
//        return false;
//      }
    }
  }

  private boolean hasInterest(Maybe<Object> packet) { return _packetVisitors[((IPacket) packet.get()).getEventType().ordinal()] != null; }

  private boolean isDataPacket(Maybe<Object> packet){
    return !(packet.get() instanceof IPacket);
  }

//  // FIXME remove this function!
  private boolean handleVisitorReturnStatus(VisitorReturnType visitorReturnStatus, Maybe<Object> currentPacketToBeReturned) {
    switch (visitorReturnStatus) {
      case NOT_MY_BUISNESS:
        // some listener has to handle this packet!!!
        // TODO turn into an exception!
        if (RuntimeProcessConfiguration.LOGGING_ENABLED) _logger.severe("Unhandled packet detected: "
                + currentPacketToBeReturned
                + " in operator "
                + getOwner().getOperatorName()
                + " on input port " + this
                + " port state: "
                + _state);
        Assertion.impossible("See log for details.");
        break;
      case PACKET_WAS_HANDLED:
        // just return the enqueued packet
        return false;
      case DEQUEUE_NEXT_PACKET:
        // this one was a signal packet that was not intended for the operator code, therefore
        // dequeue the next packet
        return true;
    }

    Assertion.impossible();
    return true;
  }

  public Arc getIncomingArc() {
    return _incomingArc == null ? null : _incomingArc.get();
  }

  public void setIncomingArc(Arc arc) {
    if (_incomingArc == null) {
      _incomingArc = new AtomicReference<Arc>(arc);
    } else {
      _incomingArc.set(arc);
    }
  }

  public boolean hasSeenLastPacket() {
    return _lastPacketReceived;
  }

  public void setHasSeenLastPacket(boolean lastPacketReceived) {
    if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
      _logger.info(getPortName() + " received last packet! -> " + lastPacketReceived);
    }
    _lastPacketReceived = lastPacketReceived;
  }

//  public Object getCurrentPacketToBeReturned() {
//    return _currentPacketToBeReturned;
//  }

//  public void setCurrentPacketToBeReturned(Maybe<Object> currentPacketToBeReturned) {
//    _currentPacketToBeReturned = currentPacketToBeReturned;
//  }

  public void registerPacketVisitor(VisitorMixin<? extends IPacket, ? extends IPacketHandler> visitor) {
    if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
      _logger.log(Level.ALL,
              "registered visitor " + visitor.getClass().getSimpleName() + " on input port " + getPortId());
    }

    _packetVisitors[visitor.getEventResponsibility().ordinal()] = visitor;
  }

  public void unregisterPacketVisitor(VisitorMixin<? extends IPacket, ? extends IPacketHandler> visitor) {
    _packetVisitors[visitor.getEventResponsibility().ordinal()] = null;
  }

  /**
   * FIXME make this type safe! DataPollOnBlockedPortEvent -> IDataPollOnBlockedPortHandler
   *
   * @param event
   * @param handler
   */
  public void registerForEvent(InputPortEvents event, IPacketHandler handler) {
    VisitorMixin visitor = _packetVisitors[event.ordinal()];
    Assertion.invariant(visitor != null);
    visitor.registerMarkerHandler(handler);
  }

  public void unregisterFromEvent(InputPortEvents event, IPacketHandler handler) {
    VisitorMixin visitor = _packetVisitors[event.ordinal()];
    Assertion.invariant(visitor != null);
    visitor.unregisterMarkerHandler(handler);
  }

  public Set<IPacketHandler> getAllPacketHandlers() {
    Set<IPacketHandler> handlers = new HashSet<IPacketHandler>();
    for (VisitorMixin visitor : _packetVisitors) {
      if (visitor != null) handlers.addAll(visitor.getAllHandlers());
    }
    return handlers;
  }

  public Set<IPacketHandler> getHandlersForEvent(InputPortEvents event) {
    VisitorMixin visitor = _packetVisitors[event.ordinal()];
    // TODO this should be an exception rather than an assertion!
    Assertion.invariant(visitor != null);
    return visitor.getAllHandlers();
  }

  public Set<IPacketHandler> getHandlersForEventGracefully(InputPortEvents event) {
    VisitorMixin visitor = _packetVisitors[event.ordinal()];

    if (visitor == null) {
      return Collections.emptySet();
    } else {
      return visitor.getAllHandlers();
    }
  }

//  public VisitorReturnType getVisitorReturnStatus() {
//    return _visitorReturnStatus;
//  }
//
//  public void setVisitorReturnStatus(VisitorReturnType visitorReturnStatus) {
//    _visitorReturnStatus = visitorReturnStatus;
//  }

  public boolean hasIncomingArc() {
    return _incomingArc != null;
  }

  /**
   * The policy is: Whoever is registered when this function is being called will be initialized
   * even if the handler gets deregistered during this initialization phase.
   */
  public void initialize() {
    // Collect all handlers
    Set<IPacketHandler> handlers = new HashSet<>();
    for (VisitorMixin visitor : _packetVisitors) {
      if (visitor != null) handlers.addAll(visitor.getAllHandlers());
    }

    // initialize all the handlers
    for (IPacketHandler packetHandler : handlers) {
      packetHandler.init();
    }
  }

  protected int skimMetaData() {
    if (getState() == PortState.BLOCKED && !isMetaPort()) {
      return 0;
    }
    Arc arc = _incomingArc.get();
    int count = 0;
    while (isMetaDataAvailable(arc)) {
      Maybe<Object> packet = arc.getData();
      Assertion.invariant(packet.isPresent());
      Assertion.invariant(!(packet.get() instanceof DataPacket) && packet.get() instanceof IStreamPacket);
      notifyPacketListeners(packet);
      count++;
    }
    return count;
  }

  private boolean isMetaDataAvailable(Arc arc) {
    Maybe<Object> d = arc.peek();
    return d.isPresent() && d.get() instanceof IMetaDataPacket;
  }

  /**
   * Only skim for meta data packets that are independent of the state of this operator. (Later
   * on we will introduce a better operator state management and then we can get rid of this
   * function again.)
   * <p>
   */
  protected int skimSpecialMetaData() {
    if (getState() == PortState.BLOCKED && !isMetaPort()) {
      return 0;
    }

    Arc arc = _incomingArc.get();
    Maybe<Object> packet = arc.peek();
    int count = 0;
    while (packet.isPresent() && packet.get() instanceof ISpecialMetaDataPacket) {
      arc.getData();
//      if(arc.remove(packet.get())) {
      Assertion.invariant(!(packet.get() instanceof DataPacket));
      notifyPacketListeners(packet);
//      }
      count++;
      packet = arc.peek();
    }
    return count;
  }

  /**
   * A closed port is not participating anymore in the processing in that is going on the
   * operator state machine.
   *
   * @return
   */
  public boolean isActive() {
    return _state != PortState.CLOSED;
  }

  @Override
  protected void setState(PortState state) {
    super.setState(state);
    if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
      _logger.info(getOwner().getID() + "->" + toString() + ": Input Port set to state: " + state);
    }
  }

  public void setComplimentaryInput() {
    _upstreamControlled = false;
  }

  public boolean isUpstreamControlled() {
    return _upstreamControlled;
  }

  public String deadlockAnalysis() {
    StringBuffer analysis = new StringBuffer();
    analysis.append("InputPort :" + this + "\n");
    analysis.append("InputPort.PortState :" + getState() + "\n");
    analysis.append("InputPort.isMetaPort :" + isMetaPort() + "\n");
    analysis.append("InputPort.hasSeenLastPacket :" + hasSeenLastPacket() + "\n");
    analysis.append("InputPort.active :" + isActive() + "\n");
    return analysis.toString();
  }

  public enum VisitorReturnType {
    PACKET_WAS_HANDLED,
    NOT_MY_BUISNESS,
    DEQUEUE_NEXT_PACKET
  }

}
