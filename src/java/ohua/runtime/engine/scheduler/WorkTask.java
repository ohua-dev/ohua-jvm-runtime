/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.sections.NotificationBasedSectionRuntime;

/**
 * It is assured that during the time that a task is executing, we never change a section. This
 * makes reasoning about the state of the computation easier when a task returns.
 * 
 * @author sertel
 * 
 */
@Deprecated
public class WorkTask implements Callable<WorkTask>
{
  private List<Work> _work = null;
  private NotificationBasedSectionRuntime _section = null;
  
  public WorkTask(NotificationBasedSectionRuntime callable, List<Work> work) {
    _section = callable;
    _work = work;
  }
  
  protected void beforeExecution() {
    for(Work w : _work) {
      OperatorCore op = w.activate();
//      _section.activateOperator(op);
    }
  }
  
  public void run() {
    beforeExecution();
    _section.call();
  }
  
  protected NotificationBasedSectionRuntime getSection() {
    return _section;
  }
  
  /**
   * At this point we trick the threadpoolexecutor because it does not want to reveal the task
   * in afterExecute().
   */
  @Override public WorkTask call() throws Exception {
    beforeExecution();
    _section.call();
    return this;
  }
  
  public List<Work> inCompleteWork() {
    List<Work> incompleteWork = new ArrayList<Work>();
    for(Work work : _work) {
      Work inComplete = work.reportIncompleteWork();
      if(inComplete != null)  incompleteWork.add(work);
    }
    return incompleteWork;
  }
}
