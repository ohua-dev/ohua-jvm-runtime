/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;

import java.util.*;

import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

public abstract class AbstractArcQueue implements Queue<Object>
{
  protected Deque<Object> _queue = null;

  public AbstractArcQueue()
  {
    initialize();
  }

  protected void initialize()
  {
    _queue = newDataQueue();
  }

  public void setMinCapacity(int capacity){
    Deque<Object> queue = configureWithMinCapacity(capacity);
    if(queue != null) _queue = queue;
  }

  protected Deque<Object> configureWithMinCapacity(int capacity){
    // can be overridden to tailor the queue impl
    return null;
  }

  abstract protected Deque<Object> newDataQueue();

  @Override
  public IStreamPacket element()
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ArcQueue.element(...) not yet implemented");
  }

  @Override
  public boolean offer(Object o)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ArcQueue.offer(...) not yet implemented");
  }

  @Override
  public Object peek()
  {
    return _queue.peek();
  }

  @Override
  public Object poll()
  {
    return _queue.poll();
  }

  @Override
  public IStreamPacket remove()
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ArcQueue.remove(...) not yet implemented");
  }

  // out-of-band support: fast travelers
  public boolean addMetaData(IStreamPacket o)
  {
    _queue.addFirst(o);
    return true;
  }

  @Override
  public boolean add(Object o)
  {
    return _queue.add(o);
  }
  
  public int getDataPacketAmount()
  {
    return _queue.size();
  }

  @Override
  public boolean addAll(Collection<? extends Object> c)
  {
    return _queue.addAll(c);
  }

  @Override
  public void clear()
  {
    _queue.clear();
  }

  @Override
  public boolean contains(Object o)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ArcQueue.contains(...) not yet implemented");
  }

  @Override
  public boolean containsAll(Collection<?> c)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ArcQueue.containsAll(...) not yet implemented");
  }

  @Override
  public boolean isEmpty()
  {
    return _queue.isEmpty();
  }
  
  @Override
  public Iterator<Object> iterator()
  {
    return new DataQueueIterator(this);
  }

  @Override
  public boolean remove(Object o)
  {
    return _queue.remove(o);
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ArcQueue.removeAll(...) not yet implemented");
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("ArcQueue.retainAll(...) not yet implemented");
  }

  @Override
  public int size()
  {
    return _queue.size();
  }

  @Override
  public Object[] toArray()
  {
    return _queue.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a)
  {
    return _queue.toArray(a);
  }
  
  public void transferTo(AbstractArcQueue q)
  {
    q._queue.addAll(_queue);
  }
  
  /**
   * Iterator methods:
   * <p>
   * This implementation will be performed on a snapshot of the data queue. It is needed because
   * there exists no concurrently linked list implementation that would allow to perform read
   * operations among all the elements of the list!
   */
  public class DataQueueIterator implements Iterator<Object>
  {
    private LinkedList<Object> _dataQueueSnapshot = new LinkedList<>();
    private int _currentIteratorIndex = 0;
    
    private AbstractArcQueue _pointer;
    
    public DataQueueIterator(AbstractArcQueue enclosingType)
    {
      _pointer = enclosingType;
    }
    
    // TODO this might be quit expensive, some form of delta snapshot would be nice!
    public void snapshot()
    {
      _dataQueueSnapshot.clear();
      _dataQueueSnapshot.addAll(_pointer);
      _currentIteratorIndex = 0;
    }
    
    public LinkedList<Object> getSnapshot()
    {
      return _dataQueueSnapshot;
    }

    @Override
    public boolean hasNext()
    {
      return _currentIteratorIndex < _dataQueueSnapshot.size();
    }

    @Override
    public Object next()
    {
      Object packet = _dataQueueSnapshot.get(_currentIteratorIndex);
      
      _currentIteratorIndex++;
      
      return packet;
    }

    @Override
    public void remove()
    {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException("DataQueueIterator.remove(...) not yet implemented");
    }
  }
}
