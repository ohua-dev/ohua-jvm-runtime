/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class UserRequest
{
  private enum RequestState
  {
    PENDING,
    SUBMITTED
  }
  
  private RequestState _state = RequestState.PENDING;
  
  private ReentrantLock _lock = new ReentrantLock();
  private Condition _submitted = _lock.newCondition();
  
  private UserRequestType _requestType = null;
  private Object _input = null;
  
  public UserRequest(UserRequestType requestType)
  {
    _requestType = requestType;
  }

  public UserRequest(UserRequestType requestType, Object input)
  {
    this(requestType);
    _input = input; 
  }
  
  public Object getInput()
  {
    return _input;
  }

  protected void submitted()
  {
    _lock.lock();
    try
    {
      _state = RequestState.SUBMITTED;
      _submitted.signal();
    }
    finally
    {
      _lock.unlock();
    }
  }

  public void waitForSubmission(long time, TimeUnit unit) throws InterruptedException
  {
    _lock.lock();
    try
    {
      while(_state != RequestState.SUBMITTED)
      {
        _submitted.await(time, unit);
      }
    }
    finally
    {
      _lock.unlock();
    }
  }
  
  public UserRequestType getRequestType()
  {
    return _requestType;
  }
}
