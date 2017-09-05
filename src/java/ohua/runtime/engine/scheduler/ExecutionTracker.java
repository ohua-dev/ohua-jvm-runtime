/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

/**
 * This class tracks the operators which are currently executing and are therefore not ready for
 * scheduling.
 * 
 * @author sertel
 * 
 */
public class ExecutionTracker
{
  private Set<OperatorID> _executing = new HashSet<OperatorID>();
  
  private AtomicBoolean execFinished = new AtomicBoolean(false);
  private ReentrantLock _executionFinishedLock = new ReentrantLock();
  private Condition _executionFinished = _executionFinishedLock.newCondition();
  
  protected boolean isReady(OperatorID op) {
    return !_executing.contains(op);
  }
  
  protected void block(OperatorID op) {
    _executing.add(op);
  }
  
  protected void unblock(OperatorID op) {
    _executing.remove(op);
  }
  
  protected void signalOperatorsReady() {
    _executionFinishedLock.lock();
    try {
      execFinished.set(true);
      _executionFinished.signalAll();
    }
    finally {
      _executionFinishedLock.unlock();
    }
  }
  
  // TODO Here we are actually not only waiting for operators to finish their computation but
  // for new work to become available! So we should change this waiting protocol here by either
  // waiting for both or just waiting for a certain time frame!
  protected void awaitReadyOperators() {
    _executionFinishedLock.lock();
    try {
      if(!execFinished.get()) _executionFinished.awaitUninterruptibly();
      execFinished.set(false);
    }
    finally {
      _executionFinishedLock.unlock();
    }
  }
  
}
