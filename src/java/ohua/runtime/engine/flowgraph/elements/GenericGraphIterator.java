/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;

import java.util.*;
import java.util.stream.*;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.InvariantBroken;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;

public abstract class GenericGraphIterator<T> implements Iterator<T>
{
//  protected final class GraphNodeComparator implements Comparator<T>
//  {
//    public int compare(GraphNode o1, GraphNode o2)
//    {
//      return o1.getUniqueID().compareTo(o2.getUniqueID());
//    }
//  }
  
  private Set<T> _visited = new LinkedHashSet<T>();
  private Set<T> _notVisited = new LinkedHashSet<T>();
  protected List<T> _graph = null;
  private T _next = null;
  protected T _possibleNext = null;
  
  private LinkedList<T> _branches = new LinkedList<T>();
  private boolean _iterateMetaComponents = false;
  
  public GenericGraphIterator(List<T> graph)
  {
    _graph = graph;
  }
  
  public GenericGraphIterator(List<T> graph, boolean iterateMetaComponents)
  {
    _iterateMetaComponents = iterateMetaComponents;
    _graph = graph;
//    initialize(graph);
  }

  protected abstract Comparator<T> getComparator();

  protected final void initialize()
  {
    List<T> graphNodes = new ArrayList<>(_graph);
    Collections.sort(graphNodes, getComparator());
    _notVisited.addAll(graphNodes);
    searchBranches();
    _next = _branches.poll();
    
    setPossibleNextOperator(_next);
    
    if(_possibleNext == null && !_branches.isEmpty())
    {
      _possibleNext = _branches.poll();
    }
  }

  
  private void searchBranches()
  {
    for(T op : _notVisited)
    {
      int numInputs = getNumGraphNodeInputs(op);
      
      if(numInputs == 0 && !_branches.contains(op))
      {
        _branches.add(op);
      }
      
    }
    
    assert !_branches.isEmpty();
  }
  
  private int getNumGraphNodeInputs(T op)
  {
    return getGraphNodeInputConnections(op).size();
  }
  
  private int getNumGraphNodeOutputs(T op)
  {
    return getGraphNodeOutputConnections(op).size();
  }
  
  public boolean hasNext()
  {
    if(_notVisited.isEmpty())
    {
      if(_possibleNext != null)
      {
        throw new InvariantBroken();
      }
      return false;
    }
    
    // first round through
    if(_next != null)
    {
      return true;
    }

    _next = findNextOperator(_possibleNext);

    //if (_next == null) throw new NullPointerException("Finding new operator returned null.");
    
    // set the possibleNext field appropriately
    if (getNumGraphNodeOutputs(_next) != 0) {
      // Assertion.invariant(_next != null);
      setPossibleNextOperator(_next);
      return true;
    }

    
    if(!_branches.isEmpty())
    {
      _possibleNext = _branches.poll();
    }
    else
    {
      _possibleNext = null;
    }
    
    return true;
  }
  
  protected T findNextOperator(T op)
  {
    Assertion.invariant(!_visited.contains(op));
    
    if(op != null && isReady(op))
    {
      return op;
    }
    else
    {
      Optional<T> branch = _branches.stream().filter(this::isReady).findFirst();

      if (branch.isPresent()) {
        _branches.removeFirstOccurrence(branch.get());
        return branch.get();
      }
    }
    // this means that there is an input that we have not been at yet but it is not registered
    // as a branch. hence, it must be a cycle. in that case we have to flag this op as ready to
    // be traversed.
    return op;
  }
  
  private boolean isReady(T op)
  {
    return getGraphNodeInputConnections(op).stream().allMatch(arc -> _visited.contains(arc.getSource()));
  }

  @SuppressWarnings("unchecked")
  private List<Arc> getGraphNodeInputConnections(T op)
  {
    List<Arc> opIn = getInputConnections(op);

    if (_iterateMetaComponents)
      return new ArrayList<>(opIn);
    else
      return opIn.stream().filter(arc -> !arc.getSource().isSystemComponent()).collect(Collectors.toList());
  }

  protected abstract List<Arc> getInputConnections(T op);

  @SuppressWarnings("unchecked")
  private List<Arc> getGraphNodeOutputConnections(T op)
  {
    if (op == null) throw new NullPointerException("Op is null");
    List<Arc> opOut = getOutputConnections(op);
    try {
      if (_iterateMetaComponents)
        return new ArrayList<>(opOut);
      else
        return opOut.stream().filter(arc -> !arc.getTarget().isSystemComponent()).collect(Collectors.toList());
    }
    catch (NullPointerException e) {
      for (int i = 0; i < opOut.size(); i ++)
        if (opOut.get(i) == null) throw new NullPointerException("Arc " + i + " for op " + getID(op) + " was null.");

      throw e;
    }
  }

  protected abstract List<Arc> getOutputConnections(T op);

  protected abstract String getID(T op);
  
  /**
   * This function finds the succeeding operator that might be the next operator to give back to
   * the user. We can not say for sure because this possibleNext guy might be an operator that
   * joins two branches and therefore we might have to walk the other branch first before giving
   * this operator to the user. Also this function registers new branches into the branch queue!
   * Mind that in the case where an operator joins AND splits the flow those new branches will
   * be added after the existing branches in the queue. Since this is a FIFO queue we not get to
   * this branches until we have walked all the branches joined by this operator!
   * @param next
   */
  @SuppressWarnings("unchecked")
  protected void setPossibleNextOperator(T next)
  {
    Assertion.invariant(getOutputConnections(_next).size() != 0, () -> getID(next));
    
    Set<T> newBranches = new HashSet<>();
    
    for(Arc arc : getGraphNodeOutputConnections(next))
    {
      // in order to support cycles we put them only in if they have not been visited yet.
      T target = getNextGraphNode(arc);
      if(_notVisited.contains(target))
      {
        newBranches.add(target);
      }
    }
    
    if(newBranches.isEmpty())
    {
      _possibleNext = null;
      return;
    }
    
    Iterator<T> it = newBranches.iterator();
    _possibleNext = it.next();
    it.remove();
    
    // this guy might actually be also in the already existing branches!
    _branches.remove(_possibleNext);
    
    // add the new branches to the queue
    for(T newBranch : newBranches)
    {
      if(!_branches.contains(newBranch) && _notVisited.contains(newBranch))
      {
        Assertion.invariant(!_visited.contains(newBranch));
        _branches.add(newBranch);
      }
    }
  }

  protected abstract T getNextGraphNode(Arc arc);
  
  public T next()
  {
    if(_next == null)
    {
      hasNext();
    }
    
    if(_next == null)
    {
      throw new NoSuchElementException();
    }
    
    T next = _next;
    _next = null;
    
    // update the lists
    _notVisited.remove(next);
    _visited.add(next);
    
    return next;
  }
  
  public void remove()
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("GraphIterator.remove(...) not yet implemented");
  }
  
}
