/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

public enum OutputPortEvents
{
  EMIT_PACKET()
  {
    @Override
    public Object getResult()
    {
      return this._result == null ? true
                                 : this._result;
    }
  },

  OUTPUT_PORT_FINISHED,
  OUTPUT_PORT_CLOSED;
  
  protected Object _args = null;
  protected Object _result = null;
  
  public void setArgs(Object args)
  {
    _args = args;
  }
  
  public Object getArgs()
  {
    return _args;
  }

  public Object getResult()
  {
    return _result;
  }
  
  public void setResult(Object result)
  {
    _result = result;
  }
}
