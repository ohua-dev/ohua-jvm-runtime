/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

/**
 * In the task-based model all assignment of work is performed through the scheduler. Therefore,
 * this queue implementation has two independent ends: one for enqueuing and one for dequeing.
 * 
 * @author sertel
 * 
 */
public class DoubleEndedArcQueue implements Queue<Object>
{
  private WorkChunk _enqueue = null;
  private WorkChunk _dequeue = null;
  
  protected void assignResultChunk(WorkChunk resultChunk) {
    _enqueue = resultChunk;
  }
  
  protected void assignWork(WorkChunk chunk) {
    Assertion.invariant(_dequeue == null);
    _dequeue = chunk;
  }

  protected WorkChunk releaseWork(){
    WorkChunk d = _dequeue;
    _dequeue = null;
    return d;
  }

  protected WorkChunk releaseResultChunk(){
    WorkChunk r = _enqueue;
    _enqueue = null;
    return r;
  }

  @Override public boolean add(Object e) {
    return _enqueue.add(e);
  }

  public void addFirst(Object e) {
    _enqueue.addFirst(e);
  }

  @Override public IStreamPacket element() {
    return _dequeue.element();
  }
  
  @Override public boolean offer(Object e) {
    return _enqueue.offer(e);
  }
  
  @Override public Object peek() {
    return _dequeue.peek();
  }
  
  @Override public Object poll() {
    return _dequeue.poll();
  }
  
  @Override public Object remove() {
    return _dequeue.remove();
  }

  @Override public boolean addAll(Collection<? extends Object> c) {
    return _enqueue.addAll(c);
  }

  /**
   * Called by the framework to check if the enqueue boundary was reached.
   * @return
   */
  @Override public int size() {
    return _enqueue == null ? 0 : _enqueue.getWorkSize();
  }

  /**
   * Called by the framework to check if the queue is empty.
   * @return
   */
  @Override public boolean isEmpty() {
    return _dequeue == null || _dequeue.isEmpty();
  }
  
  @Override public void clear() {
    releaseWork();
    releaseResultChunk();
  }
  
  @Override public boolean contains(Object o) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override public boolean containsAll(Collection<?> c) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override public Iterator<Object> iterator() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override public boolean remove(Object o) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override public boolean removeAll(Collection<?> c) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override public boolean retainAll(Collection<?> c) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  @Override public Object[] toArray() {
    // For debugging purposed only!
    return new Object[]{
            _dequeue == null ? null : _dequeue.toArray(),
            _enqueue == null ? null : _enqueue.toArray()
    };
  }
  
  @Override public <T> T[] toArray(T[] a) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
}
