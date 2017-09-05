/*
 * Copyright (c) Sebastian Ertel 2011. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

public class ConcurrentArcQueue extends AbstractArcQueue
{
  public static class OhuaLinkedBlockingQueue extends LinkedBlockingDeque<IStreamPacket> {

  }
  
  public static class OhuaBackoffQueue extends OhuaConcurrentLinkedDeque<IStreamPacket>
  {
    public IStreamPacket peek()
    {
      IStreamPacket p = super.peek();
      for(int i = 0; i < 20 && p == null; i++)
      {
        // 100 micro seconds
        try
        {
          Thread.sleep(0, 100000);
          p = super.peek();
        }
        catch(InterruptedException e)
        {
          Assertion.impossible(e);
        }
      }
      return p;
    }
  }
  
  @SuppressWarnings("rawtypes")
  private Class<? extends Deque> _impl = null;
  
  public ConcurrentArcQueue()
  {
    super();
  }
  
  @SuppressWarnings("rawtypes")
  public ConcurrentArcQueue(Class<? extends Deque> impl)
  {
    _impl = impl;
    initialize();
  }
  
  @SuppressWarnings("unchecked")
  @Override
  protected Deque<Object> newDataQueue()
  {
    try
    {
      if(_impl == null)
      {
        return new OhuaConcurrentLinkedDeque<>();
      }
      else
      {
        return _impl.newInstance();
      }
    }
    catch(Exception e)
    {
      Assertion.impossible(e);
    }
    return null;
  }

  public static class OhuaConcurrentLinkedDeque<T> extends ConcurrentLinkedDeque<T>
  {
    // this needs to be an atomic integer, not because we need an accurate value here but in a highly concurrent setting
    // it is possible that many updates to this value are pending. this leads to misunderstandings inside the operators on whether
    // they can enqueue or dequeue more. ultimately, this results in scheduling overhead. (I have seen very bad behavior (> 200 pending updates) that
    // I claim is due to this variable not being concurrent.)
    private AtomicInteger _size = new AtomicInteger();

    public int size() {
      return _size.get();
    }

    public void addFirst(T o) {
      _size.incrementAndGet();
      super.addFirst(o);
    }

    public boolean add(T o) {
      _size.incrementAndGet();
      return super.add(o);
    }

    public boolean addAll(Collection<? extends T> c) {
      _size.addAndGet(c.size());
      return super.addAll(c);
    }

    public T poll() {
      T n = super.poll();
      if (n != null)
        _size.decrementAndGet();
      return n;
    }
  }

  public static class BatchedConcurrentLinkedDeque<T> implements Deque<T>{
    public static int BATCH_SIZE = 10; // TODO set this via the runtime configuration
    private ArrayDeque<T> _senderLocal = new ArrayDeque<>(BATCH_SIZE);
    private ConcurrentLinkedDeque<ArrayDeque<T>> _concQueue = new ConcurrentLinkedDeque<>();
    private ArrayDeque<T> _receiverLocal = new ArrayDeque<>(BATCH_SIZE);
    /**
     * In a NUMA architecture it might not make sense to perform this resource pooling because of the transfer costs!
     */
    // TODO limit the max pool size when enqueuing in here
    private ConcurrentLinkedQueue<ArrayDeque<T>> _pool = new ConcurrentLinkedQueue<>();
    private AtomicInteger _size = new AtomicInteger(0);

    @Override
    public void addFirst(T t) {
      _size.incrementAndGet();
      ArrayDeque<T> a = _pool.poll();
      a = a == null ? new ArrayDeque<>(BATCH_SIZE) : a;
      a.add(t);
      _concQueue.addFirst(a);
    }

    @Override
    public void addLast(T t) {
      _size.incrementAndGet();
      _senderLocal.add(t);
      if(_senderLocal.size() == BATCH_SIZE || t instanceof IMetaDataPacket){
        /**
         * 1. approach: insert all items into queue.
         * this builds a node chain for the elements in _senderLocal and then adds them with a single atomic operation.
         * Advantages:
         *  - _receiverLocal not needed
         * Disadvantages:
         *  - only reduces the enqueue operations to the concurrent queue
         *  - creates nodes without reusing them
         */
//        _concQueue.addAll(_senderLocal);

        /**
         * 2. approach: ship the whole batch.
         *  - one concurrent call to the queue and one to the pool
         *  - reuses the batch data structures
         */
        _concQueue.add(_senderLocal);
        ArrayDeque recycledLocal = _pool.poll();
        _senderLocal = recycledLocal == null ? new ArrayDeque<>(BATCH_SIZE) : recycledLocal;
      }else{
        // nothing
      }
    }

    @Override
    public boolean offerFirst(T t) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean offerLast(T t) {
      throw new UnsupportedOperationException();
    }

    @Override
    public T removeFirst() {
      throw new UnsupportedOperationException();
    }

    @Override
    public T removeLast() {
      throw new UnsupportedOperationException();
    }

    @Override
    public T pollFirst() {
      reloadReceiverLocal();
      return pollFirstFromLocal();
    }

    private ArrayDeque<T> _emptyQueue = new ArrayDeque<>(0);
    private void reloadReceiverLocal(){
      if(_receiverLocal.isEmpty()){
        // recycle
        //_receiverLocal.clear(); --> should not be necessary because obviously the queue is already empty
        if(_receiverLocal != _emptyQueue) _pool.add(_receiverLocal);

        // get new one
        ArrayDeque<T> a = _concQueue.pollFirst();
        if(a == null){
          _receiverLocal = _emptyQueue; // the empty queue is never been written to. (we can not keep the old one because we returned it to the pool already.)
        }else{
          _receiverLocal = a;
        }
      }else{
        // all good
      }
    }

    private T pollFirstFromLocal(){
      T a = (T) _receiverLocal.pollFirst();
      if(a != null) _size.decrementAndGet(); // FIXME what if the value was null?! see my comment above about Null.NULL
      return a; // FIXME this leads to invalid semantics. you can ask the queue and it will tell you that it is not empty.
      // however, it will not be possible to deque anything because the data remains on the sender side, because the batch is not full yet!
      // the fix is to not use the isEmpty() function to understand whether the result of a poll() is actually saying "no data available" or
      // actually dequeing a null value.
    }

    @Override
    public T pollLast() {
      throw new UnsupportedOperationException();
    }

    @Override
    public T getFirst() {
      throw new UnsupportedOperationException();
    }

    @Override
    public T getLast() {
      throw new UnsupportedOperationException();
    }

    @Override
    public T peekFirst() {
      reloadReceiverLocal();
      return (T) _receiverLocal.peekFirst();
    }

    @Override
    public T peekLast() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T t) {
      addLast(t);
      return true;
    }

    @Override
    public boolean offer(T t) {
      return offerLast(t);
    }

    @Override
    public T remove() {
      return removeFirst();
    }

    @Override
    public T poll() {
      return pollFirst();
    }

    @Override
    public T element() {
      return getFirst();
    }

    @Override
    public T peek() {
      return peekFirst();
    }

    @Override
    public void push(T t) {
      addFirst(t);
    }

    @Override
    public T pop() {
      return removeFirst();
    }

    @Override
    public boolean remove(Object o) {
      return removeFirstOccurrence(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      _senderLocal.clear();
      _concQueue.clear();
      _receiverLocal.clear();
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return _size.get();
    }

    @Override
    public boolean isEmpty() {
      return _size.get() == 0;
    }

    @Override
    public Iterator<T> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
      Object[] sender = _senderLocal.toArray();
      Object[] conc =  _concQueue.toArray();
      Object[] receiver = _receiverLocal.toArray();
      Object[] finalTarget = new Object[sender.length + conc.length + receiver.length];
      System.arraycopy(receiver, 0, finalTarget, 0, receiver.length);
      System.arraycopy(conc, 0, finalTarget, receiver.length, conc.length);
      System.arraycopy(sender, 0, finalTarget, receiver.length + conc.length, sender.length);
      return finalTarget;
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> descendingIterator() {
      throw new UnsupportedOperationException();
    }
  }

}
