/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;

import java.util.Optional;
import java.util.Set;

/**
 * Created by sertel on 1/24/17.
 */
public abstract class AbstractOperatorScheduler<T extends AbstractOperatorRuntime> {

  protected Set<T> _graph = null;

  public AbstractOperatorScheduler(Set<T> graph){
    _graph = graph;
  }

  public final void initialize(){
    init();
  }

  protected void init() {}

  public final void runExecutionStep() {
    beforeExecutionStep();
    Optional<T> next = schedule();
    while(next.isPresent()) {
      T op = next.get();
      prepareOpForExecution(op);
      while(op.isActive()) op.runOperatorStep();
      handleDoneExecution(op);
      next = schedule();
    }
    afterExecutionStep();
  }

  protected void beforeExecutionStep() {}
  protected void afterExecutionStep() {}

  protected void prepareOpForExecution(T op){
    op.activate();
  }

  protected abstract void handleDoneExecution(T op);
  protected abstract Optional<T> schedule();
}
