/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.*;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.AbstractArcQueue;

/**
 * Instead of a direct arc, this is a chunk of work which gets assigned to an arc
 * implementation.
 * @author sertel
 * 
 */
public class WorkChunk extends AbstractArcQueue
{
  /**
   * This is pending work that is not present in this work chunk. This is needed during arc boundary checking.
   */
  private int _lowerWorkBound = 0;
  private int _memoizedSize = 0;

  public WorkChunk(){
    super();
  }

  public WorkChunk(int capacity){
    _queue = new ArrayDeque<>(capacity);
  }

  @Override
  protected Deque<Object> newDataQueue() {
    return new ArrayDeque<>();
  }

//  protected Deque<Object> configureWithMinCapacity(int capacity){
//    return new ArrayDeque<>(capacity);
//  }

  protected WorkChunk memorize(){
    _memoizedSize = super.getDataPacketAmount();
    return this;
  }

  public int getMemoizedSize() { return _memoizedSize; }

  protected WorkChunk setLowerWorkBound(int bound){
    _lowerWorkBound = bound;
    return this;
  }

  protected int getWorkSize(){
    return _lowerWorkBound + super.size();
  }

  protected void addFirst(Object o){
    _queue.addFirst(o);
  }

  public boolean add(Object o) {
    boolean res = super.add(o);
    Assertion.invariant(res);
    return res;
  }

  public boolean addAll(Collection<? extends Object> c)
  {
    boolean r = super.addAll(c);
    Assertion.invariant(c.isEmpty() || r);
    return r;
  }

  @Override
  public Iterator<Object> iterator()
  {
    return _queue.iterator();
  }

}
