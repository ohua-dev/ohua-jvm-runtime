/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.flowgraph.elements.AbstractArcQueue;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractArcImpl;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

import java.util.List;

/**
 * Created by sertel on 1/23/17.
 */
public class WorkBasedAsynchronousArc extends AbstractArcImpl {

  private DoubleEndedArcQueue _queue = new DoubleEndedArcQueue();

  public Maybe<Object> getData() {
    return _queue.isEmpty() ? super.get() : super.get(_queue.poll());
  }

  @Override public void enqueueBatch(List<? extends IStreamPacket> batch) {
    _queue.addAll(batch);
  }

  protected boolean enqueue(Object dataPacket) {
    return _queue.add(dataPacket);
  }

  public void assignWork(WorkChunk chunk) {
    _queue.assignWork(chunk);
  }

  public void assignResultChunk(WorkChunk chunk){
    _queue.assignResultChunk(chunk);
  }

  public WorkChunk releaseWork() {
    return _queue.releaseWork();
  }

  public WorkChunk releaseResultChunk(){
    return _queue.releaseResultChunk();
  }

  public boolean isArcEmpty() {
    return _queue.isEmpty();
  }

  @Override
  public void transferTo(AbstractArcImpl arcImpl) {
    // TODO
  }

  public Maybe<Object> peek() {
    return _queue.isEmpty() ? super.get() : super.get(_queue.peek());
  }

  @Override
  protected void enqueueMetaData(IStreamPacket metaDataPacket) {
    _queue.addFirst(metaDataPacket);
  }

  @Override
  public int getLoadEstimate() {
    return _queue.size();
  }

  @Override
  public void sweep() {
    _queue.clear();
  }

  @Override
  public AbstractArcQueue.DataQueueIterator getDataIterator() {
    throw new UnsupportedOperationException();
  }

}
