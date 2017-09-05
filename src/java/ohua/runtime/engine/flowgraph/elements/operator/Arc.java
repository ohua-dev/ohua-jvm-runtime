/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.flowgraph.elements.AbstractArcQueue.DataQueueIterator;
import ohua.runtime.engine.flowgraph.elements.ArcID;
import ohua.runtime.engine.flowgraph.elements.ArcID.ArcIDGenerator;
import ohua.runtime.engine.flowgraph.elements.abstraction.GraphArc;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class Arc implements GraphArc<OperatorCore> {

  protected Logger _logger = Logger.getLogger(getClass().getCanonicalName());
  protected AtomicReference<OutputPort> _sourcePort = new AtomicReference<>();
  protected AtomicReference<InputPort> _targetPort = new AtomicReference<>();
  private ArcID _arcId = null;

  /*
   * We use this nob to control the backoff of the enqueuing operator. This is related to the
   * continuations.
   */
  private int _arcBoundary = 200;
  // graph analysis fields
  private int _level = 1;
  private ArcType _type = ArcType.FORWARD_EDGE;

  private AbstractArcImpl _arcImpl;
  public Arc(OutputPort source, InputPort target) {
    this(source, target, ArcIDGenerator.generateNewArcID());
  }
  
  public Arc(OutputPort source, InputPort target, ArcID arcID) {
    setSourcePort(source);
    setTargetPort(target);

    _arcId = arcID;
  }

  public Arc() {
    // a default constructor.
    _arcId = ArcIDGenerator.generateNewArcID();
  }

  public AbstractArcImpl getImpl() {
    return _arcImpl;
  }

  public void setImpl(AbstractArcImpl arcImpl) {
    _arcImpl = arcImpl;
  }

  public OutputPort getSourcePort() {
    return _sourcePort.get();
  }

  public void setSourcePort(OutputPort sourcePort) {
    _sourcePort.set(sourcePort);
    _sourcePort.get().addArc(this);
  }

  public InputPort getTargetPort() {
    return _targetPort.get();
  }

  public void setTargetPort(InputPort targetPort) {
    _targetPort.set(targetPort);
    if (targetPort.getIncomingArc() != null) {
      throw new IllegalArgumentException("Port " + targetPort.toString() + " is already connected to port "
              + targetPort.getIncomingArc().getSourcePort().toString()
              + ". New source port: " + targetPort.toString());
    }
    _targetPort.get().setIncomingArc(this);
  }

  public Maybe<Object> getData() {
    Maybe<Object> m = _arcImpl.getData();
    return m.isPresent() ? Maybe.value(m, convertNULLtonull(m.get())) : m;
  }

  public Maybe<Object> peek() {
    Maybe<Object> m = _arcImpl.peek();
    return m.isPresent() ? Maybe.value(m, convertNULLtonull(m.get())) : m;
  }

  private enum Null{
    NULL
  }

  private Object convertNULLtonull(Object o){
    return o == Null.NULL ? null : o;
  }

  private Object convertnulltoNULL(Object o){
    return o == null ? Null.NULL : o;
  }

  /**
   * This function will enqueue a data packet into the queue of an arc. It also provides a
   * parameter that regulates the flow inside the system. The inserting operator can choose to
   * ignore this return value which will result in buffering and less flow behavior.
   *
   * @param dataPacket
   * @return false - stop enqueuing into this arc! (boundary reached)
   */
  protected boolean enqueue(Object dataPacket) {
    _arcImpl.enqueue(convertnulltoNULL(dataPacket));
    return _arcImpl.getLoadEstimate() < _arcBoundary;
  }

  /**
   * Careful with this function! If you want to know whether something can be dequed then rather use peek()!
   * @return
   */
  public boolean isQueueEmpty() {
    return _arcImpl.isArcEmpty();
  }

  public OperatorCore getSource() {
    return _sourcePort.get().getOwner();
  }

  public OperatorCore getTarget() {
    return _targetPort.get().getOwner();
  }

  // TODO run a callback to the other operator here that when the next packet is to be enqueued
  // it is suppose to also look for the metadata enqueued on this arc. this avoids involving the
  // scheduling when propagating metadata!
  protected void enqueueMetaData(IStreamPacket metaDataPacket) {
    _arcImpl.enqueueMetaData(metaDataPacket);
  }

  public void enqueueBatch(List<? extends IStreamPacket> batch) {
    _arcImpl.enqueueBatch(batch);
  }

  public ArcID getArcId() {
    return _arcId;
  }

  public int getLevel() {
    return _level;
  }

  public void setLevel(int level) {
    _level = level;
  }

  public ArcType getType() {
    return _type;
  }

  public void setType(ArcType type) {
    _type = type;
  }

  public void sweep() {
    _arcImpl.sweep();
  }

  public void detachFromSource() {
    _sourcePort.get().getOutgoingArcs().remove(this);
    _sourcePort.set(null);
  }

  public void reconnectSource(OutputPort newOutPort) {
    _sourcePort.get().getOutgoingArcs().remove(this);
    setSourcePort(newOutPort);
  }

  public void detachFromTarget() {
    _targetPort.get().setIncomingArc(null);
    _targetPort.set(null);
  }

  public void reconnectTarget(InputPort newInPort) {
    newInPort.setIncomingArc(this);
    InputPort oldTargetPort = _targetPort.get();
    _targetPort.set(newInPort);

    // FIXME I assume that this might fail in a highly concurrent deployment because other
    // operators might still have references and try activations. When this happens then these
    // operators will find unconnected input ports and fail with an NPE.

    // Thinking about it again: Activations happen on arc basis not operator basis! That is,
    // whenever I reconnected
    // this target properly (as above) then every subsequent activation using this arc will
    // succeed! That means it is ok to nullify this arc reference below!
    oldTargetPort.setIncomingArc(null);
  }

  public int getLoadEstimate() {
    return _arcImpl.getLoadEstimate();
  }

  public int getArcBoundary() {
    return _arcBoundary;
  }

  public int getRemainingCapacityEstimate() {
    return getArcBoundary() - getLoadEstimate();
  }

  public void setArcBoundary(int arcBoundary) {
    _arcBoundary = arcBoundary;
    _arcImpl.setMinCapacity(_arcBoundary);
  }

  public void transferTo(Arc disconnectedArc) {
    _arcImpl.transferTo(disconnectedArc._arcImpl);
  }

  public enum ArcType {
    FORWARD_EDGE,
    FEEDBACK_EDGE,
    CYCLE_START
  }

  // for debugging purposes only!
  public String stringifyData(){
    StringBuilder strBuf = new StringBuilder();
    DataQueueIterator it = _arcImpl.getDataIterator();
    it.snapshot();
    int i = 0;
    Object last = null;
    while(it.hasNext()){
      if(i == 200) strBuf.append("... \n");
      else if (i > 200) last = it.next();
      else strBuf.append(it.next()).append("\n");
      i++;
    }

    if(last != null) strBuf.append(it.next()).append("\n");

    return strBuf.toString();
  }
}
