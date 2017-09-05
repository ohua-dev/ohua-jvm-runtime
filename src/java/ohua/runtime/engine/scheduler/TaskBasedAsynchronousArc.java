/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.AsynchronousArcImpl;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

import java.util.List;

/**
 * This arc implementation notifies the scheduler on newly available work and allows new work to
 * be assigned.
 * @author sertel
 * 
 */
@Deprecated
public class TaskBasedAsynchronousArc extends AsynchronousArcImpl
{
  private DoubleEndedArcQueue _queue = new DoubleEndedArcQueue();
  
  // granularity of the work to be reported to the scheduler
  private int _notificationFrequency = 10;
  private int _dataSinceLastNotification = 0;
  
  public TaskBasedAsynchronousArc(Arc arc, int notificationFrequency) {
    super(arc);
    _notificationFrequency = notificationFrequency;
  }
  
  public void setWorkChunkSize(int workChunkSize) {
    _notificationFrequency = workChunkSize;
  }
  
  public Maybe<Object> getData() {
    return _queue.isEmpty() ? super.get() : super.get(_queue.poll());
  }
  
  @Override public void enqueueBatch(List<? extends IStreamPacket> batch) {
    _queue.addAll(batch);
    _dataSinceLastNotification += batch.size();
    
    if(_dataSinceLastNotification >= _notificationFrequency) {
      super.notifyDequeueNeeded();
      _dataSinceLastNotification = 0;
    }
  }
  
  protected boolean enqueue(Object dataPacket) {
    _queue.add(dataPacket);
    _dataSinceLastNotification++;
    
    if(_dataSinceLastNotification == _notificationFrequency) {
      super.notifyDequeueNeeded();
      _dataSinceLastNotification = 0;
    }

    // FIXME
//    return !_arc.isBlocking();
    return false;
  }

  protected void assignWork(WorkChunk chunk) {
//    _queue.assignWorkChunk(chunk);
  }

  public WorkChunk retrieveWork() {
//    return _queue.exchangeWorkChunk();
    return null;
  }

  /**
   * Work which was not processed by the current run and is reported back to the scheduler.
   */
  public void releaseUnfinishedWork() {
//    _queue.relesaseWork();
  }

  public boolean isArcEmpty() {
    return _queue.isEmpty();
  }
  
  public boolean hasPendingWork() {
    return _dataSinceLastNotification > 0;
  }
  
  public Maybe<Object> peek() {
    return _queue.isEmpty() ? super.get() : super.get(_queue.peek());
  }
  
}
