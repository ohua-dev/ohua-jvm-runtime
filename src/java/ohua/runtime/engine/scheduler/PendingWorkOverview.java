/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ohua.runtime.engine.ReadReference;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;
import ohua.runtime.engine.sections.SectionGraph;

/**
 * Limited view on the pending work to be passed to the scheduling algorithms.
 * 
 * @author sertel
 * 
 */
@Deprecated
public class PendingWorkOverview
{
  private ReadReference<Map<OperatorID, WorkQueue>> _pendingWorkRef = null;
  private ReadReference<ExecutionTracker> _execTrackerRef = null;
  private ReadReference<SectionGraph> _secGraphRef = null;
  
  public PendingWorkOverview(ReadReference<Map<OperatorID, WorkQueue>> pendingWorkRef,
                             ReadReference<ExecutionTracker> execTrackerRef,
                             ReadReference<SectionGraph> secGraphRef)
  {
    _pendingWorkRef = pendingWorkRef;
    _execTrackerRef = execTrackerRef;
    _secGraphRef = secGraphRef;
  }
  
  public Set<OperatorID> getReadyOperators() {
    // TODO having to compute this over and over again is pretty time-consuming and should be
    // changed soon.
    Set<OperatorID> pendingWork = new HashSet<OperatorID>();
    for(Map.Entry<OperatorID, WorkQueue> entry : _pendingWorkRef.get().entrySet()) {
      if(isOperatorBlocked(entry.getKey())) {
        continue;
      }
      
      if(isOperatorExecuting(entry.getKey())) {
        continue;
      }
      
      if(entry.getValue().size() > 0) {
        pendingWork.add(entry.getKey());
      }
    }
    return pendingWork;
  }
  
  private boolean isOperatorExecuting(OperatorID op) {
    return !_execTrackerRef.get().isReady(op);
  }
  
  private boolean isOperatorBlocked(OperatorID op) {
    // FIXME this seems wrong to me because an operator is blocked depending on the work stack
    // that it has gathered. therefore, this question should not be answered by the operator but
    // via the _pendingWorkRef!
    OperatorCore opCore = _secGraphRef.get().findOperator(op);
    // FIXME
//    return opCore.isOperatorBlocked();
    return false;
  }
  
  public int getWorkQueueSize(OperatorID op) {
    return _pendingWorkRef.get().get(op).size();
  }
  
  public int getWorkSize(OperatorID op) {
    return _pendingWorkRef.get().get(op).getInputDataCount();
  }
}
