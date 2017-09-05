/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import ohua.runtime.engine.ReadReference;
import ohua.runtime.engine.exceptions.Assertion;

@Deprecated
public class TaskExecutor extends ScheduledThreadPoolExecutor
{
  private ReadReference<ExecutionTracker> _execTrackerRef = null;
  private ReadReference<TaskScheduler> _schedulerRef = null;

  public TaskExecutor(int corePoolSize,
                      ReadReference<ExecutionTracker> execTrackerRef,
                      ReadReference<TaskScheduler> schedulerRef)
  {
    super(corePoolSize);
    _execTrackerRef = execTrackerRef;
    _schedulerRef = schedulerRef;
  }
//
//  // FIXME instead of doing this here just do it in the task scheduler itself. just enqueue the finished WorkTask into a done queue.
//  @SuppressWarnings("rawtypes") protected void afterExecute(Runnable r, Throwable t) {
//    super.afterExecute(r, t);
//
//    WorkTask task = null;
//    // the Java Concurrency API is broken here. see comments and bug reports in internet.
//    try {
//      task = (WorkTask) ((FutureTask) r).get();
//    }
//    catch(Exception e) {
//      // this is also totally stupid because what does the second parameter of this function
//      // do?!
//      e.printStackTrace();
//      Assertion.impossible(e);
//    }
//
//    List<Work> inCompleteWork = task.inCompleteWork();
//    if(!inCompleteWork.isEmpty()) _schedulerRef.get().notifyIncompleteWork(inCompleteWork);
//
//    for(OperatorCore op : task.getSection().getOperators()) {
//      if(op.hasFinishedComputation()) {
//        // FIXME We have to also handle the case whether multiple operators are located on a
//        // single section. Now the first op finished but others did not. -> This case is caught
//        // by the below else-if statement!
//        _schedulerRef.get().notifyFinishedOperator(op);
//      }
//      else if(op.isInputComplete()) {
//        // FIXME We are resubmitting this stuff here but we are not sure whether we not already
//        // have done so!
//        _schedulerRef.get().notifyWorkReady(op.getId(), new TargetWork(op));
//      }
//      else {
//        // this branch is covered by handling the incomplete work before this loop
//      }
//    }
//
//    Assertion.invariant(task != null);
//    for(OperatorCore op : task.getSection().getOperators()) {
//      _execTrackerRef.get().unblock(op.getId());
//    }
//    _execTrackerRef.get().signalOperatorsReady();
//  }
}
