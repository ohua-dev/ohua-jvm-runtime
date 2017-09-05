package ohua.runtime.engine.flowgraph.elements;

import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

/**
 * TODO This is overhead as we really only have contention at the head of the queue. Check out
 * ConcurrentLinkedQueue and implement addFirst for it!
 * @author sebastian
 * 
 */
@Deprecated
public class ConcurrentLinkedList extends LinkedList<IStreamPacket>
{
  private ReentrantLock _lock = new ReentrantLock();
  
  private volatile int _size = 0;
  
  @Override
  public void addFirst(IStreamPacket packet)
  {
    try
    {
      _lock.lock();
      super.addFirst(packet);
      _size++;
    }
    finally
    {
      _lock.unlock();
    }
  }
  
  @Override
  public void addLast(IStreamPacket packet)
  {
    try
    {
      _lock.lock();
      super.addLast(packet);
      _size++;
    }
    finally
    {
      _lock.unlock();
    }
  }
  
  @Override
  public IStreamPacket getFirst()
  {
    try
    {
      _lock.lock();
      if(isEmpty())
      {
        return null;
      }
      return super.getFirst();
    }
    finally
    {
      _lock.unlock();
    }
  }
  
  @Override
  public IStreamPacket removeFirst()
  {
    try
    {
      _lock.lock();
      if(isEmpty())
      {
        return null;
      }
      _size--;
      return super.removeFirst();
    }
    finally
    {
      _lock.unlock();
    }
  }
  
  @Override
  public int size()
  {
    return _size;
  }
  
  @Override
  public boolean isEmpty()
  {
    return _size < 1;
  }
}
