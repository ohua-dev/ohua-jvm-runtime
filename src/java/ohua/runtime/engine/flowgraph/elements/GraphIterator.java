/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.Arc.ArcType;
import ohua.runtime.engine.operators.system.UserGraphEntranceOperator;

// FIXME this thing should at some point just go away and the Generic version should be used!
public class GraphIterator implements Iterator<OperatorCore>
{
  private Set<OperatorCore> _visited = new HashSet<OperatorCore>();
  private Set<OperatorCore> _notVisited = new HashSet<OperatorCore>();
  @SuppressWarnings("unused") private FlowGraph _graph = null;
  private OperatorCore _next = null;
  private OperatorCore _possibleNext = null;
  
  private LinkedList<OperatorCore> _branches = new LinkedList<OperatorCore>();
  
  public GraphIterator(FlowGraph graph) {
    _graph = graph;
    List<OperatorCore> containedGraphNodes = graph.getContainedGraphNodes();
    // Collections.sort(containedGraphNodes);
    _notVisited.addAll(containedGraphNodes);
    searchBranches();
    _next = _branches.poll();
    
    assert !_next.getOutputPorts().isEmpty();
    setPossibleNextOperator(_next);
  }
  
  private void searchBranches() {
    for(OperatorCore op : _notVisited) {
      if(getNumInputPorts(op) == 0 && !_branches.contains(op)) {
        _branches.add(op);
      }
    }
    
    Assertion.invariant(!_branches.isEmpty());
  }
  
  private int getNumInputPorts(OperatorCore op) {
    int numInputPorts = op.getNumInputPorts();
    if(op.getOperatorAlgorithm() instanceof UserGraphEntranceOperator) {
      return 0;
    }
    return numInputPorts;
  }
  
  public boolean hasNext() {
    if(_notVisited.isEmpty()) {
      // either we are at the end of the graph or we just hit a cycle start node
      assert _possibleNext == null || _visited.contains(_possibleNext);
      return false;
    }
    
    // first round through
    if(_next != null) {
      return true;
    }
    
    _next = findNextOperator(_possibleNext);
    
    // set the possibleNext field appropriately
    if(setPossibleNextOperator(_next)) {
      // FIXME shouldn't this the set the next operator actually?!
      return true;
    }
    
    if(!_branches.isEmpty()) {
      _possibleNext = _branches.poll();
    }
    else {
      _possibleNext = null;
    }
    
    return true;
  }
  
  public OperatorCore findNextOperator(OperatorCore op) {
    Assertion.invariant(!_visited.contains(op));
    
    if(isReady(op)) {
      return op;
    }
    else {
      for(int i = 0; i < _branches.size(); i++) {
        OperatorCore branch = _branches.get(i);
        if(isReady(branch)) {
          return _branches.remove(i);
        }
      }
    }
    
    // we hit a cycle-start node
    // FIXME this probably wants to become more explicit by looking at the arc type
    Assertion.invariant(_branches.isEmpty());
    return op;
  }
  
  private boolean isReady(OperatorCore op) {
    for(InputPort inp : op.getInputPorts()) {
      OperatorCore preceedingOp = inp.getIncomingArc().getSourcePort().getOwner();
      if(!_visited.contains(preceedingOp)) {
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * This function finds the succeeding operator that might be the next operator to give back to
   * the user. We can not say for sure because this possibleNext guy might be an operator that
   * joins two branches and therefore we might have to walk the other branch first before giving
   * this operator to the user. Also this function registers new branches into the branch queue!
   * Mind that in the case where an operator joins AND splits the flow those new branches will
   * be added after the existing branches in the queue. Since this is a FIFO queue we not get to
   * this branches until we have walked all the branches of joined by this operator!
   * @param next
   */
  private boolean setPossibleNextOperator(OperatorCore next) {
//    assert !next.getOutputPorts().isPresent();
    
    List<OperatorCore> newBranches = new ArrayList<OperatorCore>();
    
    for(int i = 0; i < next.getOutputPorts().size(); i++) {
      List<Arc> outArcs = next.getOutputPorts().get(i).getOutgoingArcs();
      for(int j = 0; j < outArcs.size(); j++) {
        if(outArcs.get(j).getType() != ArcType.FEEDBACK_EDGE) {
          OperatorCore o = outArcs.get(j).getTargetPort().getOwner();
          if(!newBranches.contains(o)) {
            newBranches.add(o);
          }
        }
      }
    }
    
    if(!newBranches.isEmpty()) {
      _possibleNext = newBranches.remove(0);
      // this guy might actually be also in the already existing branches!
      _branches.remove(_possibleNext);
      
      // add the new branches to the queue
      for(OperatorCore newBranch : newBranches) {
        if(!_branches.contains(newBranch)) {
          _branches.add(newBranch);
        }
      }
      return true;
    }
    else
      return false;
  }
  
  public OperatorCore next() {
    if(_next == null) {
      hasNext();
    }
    
    if(_next == null) {
      throw new NoSuchElementException();
    }
    
    OperatorCore next = _next;
    _next = null;
    
    // update the lists
    _notVisited.remove(next);
    _visited.add(next);
    
    return next;
  }
  
  public void remove() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("GraphIterator.remove(...) not yet implemented");
  }
  
}
