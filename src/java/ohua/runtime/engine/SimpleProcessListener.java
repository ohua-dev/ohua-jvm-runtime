/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleProcessListener implements OhuaProcessListener {
  private boolean _isDone = false;
  private Throwable _t = null;
  
  private ReentrantLock _lock = new ReentrantLock();
  private Condition _isDoneCondition = _lock.newCondition();
  
  public void reset() {
    _isDone = false;
    _t = null;
  }
  
  public void completed(UserRequest request, Throwable t) {
    if(request.getRequestType() != UserRequestType.INITIALIZE
       && request.getRequestType() != UserRequestType.FINISH_COMPUTATION
       && request.getRequestType() != UserRequestType.SHUT_DOWN)
    {
      if(t != null) {
        complete(t);
      } else {
        return;
      }
    } else {
      complete(t);
    }
    
  }
  
  private void complete(Throwable t) {
    _lock.lock();
    try {
      _isDone = true;
      _t = t;
      _isDoneCondition.signalAll();
    }
    finally {
      _lock.unlock();
    }
    
  }
  
  public void awaitProcessingCompleted() throws Throwable {
    _lock.lock();
    try {
      while(!_isDone) {
        _isDoneCondition.awaitUninterruptibly();
      }
    }
    finally {
      _lock.unlock();
    }
    if(_t != null) throw _t;
  }
  
  public void awaitAndReset() throws Throwable {
    awaitProcessingCompleted();
    reset();
  }
}
