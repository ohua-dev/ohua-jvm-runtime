/*
 * Copyright (c) Sebastian Ertel 2012. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.AbstractArcQueue;
import ohua.runtime.engine.flowgraph.elements.AbstractArcQueue.DataQueueIterator;
import ohua.runtime.engine.flowgraph.elements.ArcQueue;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

import java.util.List;

public class AsynchronousArcImpl extends AbstractNotificationBasedArcImpl {

  /*
 * We use this nob to control the concurrent activation of downstream/upstream operators. This
 * is related to concurrent execution.
 */
  @Deprecated // use the setter! // TODO remove because only used in tests
  public static int ACTIVATION_MARK = 100;
  private int _activationMark = ACTIVATION_MARK;

  // FIXME be carefull here because setting it to -1 will surpress any notifications to
  // upstream!
  // per default this optimization is disabled
  private int _enqueueWatermark = -1;

  // This kind of queue guarantees us wait-free, non-blocking synchronization!
  private AbstractArcQueue _queue = new ArcQueue();

  public AsynchronousArcImpl(Arc arc) {
    super(arc);
  }
  
  public void exchangeQueue(AbstractArcQueue q) {
    _queue = q;
  }

  protected void setMinCapacity(int capacity){
   _queue.setMinCapacity(capacity);
  }

  public void setActivationMark(int activationMark){
    _activationMark = activationMark;
  }

  public int getActivationMark(){
    return _activationMark;
  }

  @Override
  public Maybe<Object> getData() {
    /**
     * Turns out that at least Java's LinkedList is broken by design because it supports null values but the poll() call
     * actually returns also null when the queue is empty. So it is unclear what this function is actually saying.
     * <p/>
     * Here is my work around for now:
     */
    Maybe<Object> data = _queue.peek() == null ? super.get() : super.get(_queue.poll());

//    if(_arc.getState() == ArcState.BLOCKING && _queue.getDataPacketAmount() <= _arc.getArcBoundary()) {
//      _arcStateRef.set(ArcState.NORMAL);
//      // _logger.log(Level.ALL, "arc=" + _arcId + ": state changed to NORMAL");
//    }
//
    // aggressive scheduling support to achieve fully loaded arcs and high latency. goal
    // here: let a retriever activate a downstream op. -> triggers only one notifications in the
    // ConcurrentPipelineScheduling case. so no more massive activations here.
    // FIXME nobody sets this water mark! so, do we need this still???
    if(_queue.getDataPacketAmount() <= _activationMark) {
      super.notifyDataNeeded();
    }

    activateUpstream();
    return data;
  }

  @Override
  public boolean isBlocking() {
    return _queue.getDataPacketAmount() > _arc.getArcBoundary();
  }

  @Override
  protected boolean enqueue(Object dataPacket) {
    // System.out.println("add called on queue for the " + count++ + " time");
    _queue.add(dataPacket);
    
    activateDownstream();

    /**
     * This is the condition for a pipelined execution.
     */
    if(_activationMark <= _queue.getDataPacketAmount()) {
      super.notifyDataAvailable();
    }

    /**
     * This is the condition for a batched execution.
     */
    if(_queue.getDataPacketAmount() > _arc.getArcBoundary()) {
      //_arcStateRef.set(ArcState.BLOCKING);
      // _logger.log(Level.ALL, "arc=" + _arcId + ": state changed to " + _state);
      return false;
    } else {
//      _arc.notifyDequeueNeeded();
      return true;
    }
  }

  @Override
  public boolean isArcEmpty() {
    return _queue.isEmpty();
  }
  
  // TODO run a callback to the other operator here that when the next packet is to be enqueued
  // it is suppose to also look for the metadata enqueued on this arc. this avoids involving the
  // scheduling when propagating metadata!
  @Override
  protected void enqueueMetaData(IStreamPacket metaDataPacket) {
    _queue.addMetaData(metaDataPacket);
  }

  @Override
  public void enqueueBatch(List<? extends IStreamPacket> batch) {
    _queue.addAll(batch);
  }

  public boolean remove(Object packet) {
    return _queue.remove(packet);
  }

  @Override
  public int getLoadEstimate() {
    return _queue.getDataPacketAmount();
  }

  @Override
  public void sweep() {
    _queue.clear();
  }

  @Override
  public Maybe<Object> peek() {
    /**
     * See my comment in poll().
     */
    Object p = _queue.peek();
    return _queue.peek() == null ? super.get() : super.get(p);
  }
  
  @Override
  public DataQueueIterator getDataIterator() {
    return (DataQueueIterator) _queue.iterator();
  }

  @Override
  public void transferTo(AbstractArcImpl arcImpl) {
    Assertion.invariant(arcImpl instanceof AsynchronousArcImpl);
    _queue.transferTo(((AsynchronousArcImpl) arcImpl)._queue);
  }

  @Override
  public String toString() { return "async-arc -> boundary: " + _arc.getArcBoundary(); }

  public int getEnqueueWatermark() {
    return _enqueueWatermark;
  }

  public void setEnqueueWatermark(int enqueueWatermark) {
    _enqueueWatermark = enqueueWatermark;
  }

}
