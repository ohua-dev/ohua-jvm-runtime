/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import ohua.runtime.engine.flowgraph.elements.ArcID;

/**
 * Since schedulers become an extension point of the engine and work is submitted in chunks, we
 * must enforce FIFO semantics across work chunks for the same arc of an operator. We achieve
 * this by providing a notion of readiness.
 * 
 * @author sertel
 * 
 */
public class WorkQueue
{
  // this is a concurrent data structure because the scheduler might dequeue
  // from here while a task reports the arrival of new data.
  private Map<ArcID, ConcurrentLinkedDeque<Work>> _pendingWork = new HashMap<ArcID, ConcurrentLinkedDeque<Work>>();
  
  // private ConcurrentLinkedDeque<Work> _pendingWork = new ConcurrentLinkedDeque<Work>();
  
  protected WorkQueue(Set<ArcID> locations) {
    for(ArcID location : locations) {
      _pendingWork.put(location, new ConcurrentLinkedDeque<Work>());
    }
  }
  
  protected void add(Work work) {
    _pendingWork.get(work.getLocationReference()).offer(work);
  }
  
  /**
   * In order to guarantee forward progress for an operator we have to provide work available
   * for all its input ports.
   * @return
   */
  protected List<Work> get() {
    List<Work> work = new ArrayList<Work>();
    for(ConcurrentLinkedDeque<Work> wq : _pendingWork.values()) {
      if(!wq.isEmpty()) work.add(wq.poll());
    }    
    return work;
  }
  
  protected void prependUnfinished(Work work) {
    _pendingWork.get(work.getLocationReference()).addFirst(work);
  }
  
  protected int size() {
    int size = 0;
    for(ConcurrentLinkedDeque<Work> wq : _pendingWork.values()) {
      size += wq.size();
    }
    return size;
  }
  
  protected int getInputDataCount() {
    int total = 0;
    for(ConcurrentLinkedDeque<Work> wq : _pendingWork.values()) {
      total += getInputDataCount(wq);
    }
    return total;
  }
  
  protected int getInputDataCount(ConcurrentLinkedDeque<Work> wq) {
    int total = 0;
    for(Work work : wq) {
      total += work.size();
    }
    return total;
  }
  
}
