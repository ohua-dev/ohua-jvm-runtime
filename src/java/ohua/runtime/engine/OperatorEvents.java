/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;


public enum OperatorEvents
{
  // FIXME This event is obsolete!
  BLOCKED_OPERATOR,
  
  USER_OPERATOR_EXECUTION,
  USER_OPERATOR_RETURNED;

  // note that enums are singletons!
  private Map<OperatorID, Set<IOperatorEventListener>> _listeners = new HashMap<OperatorID, Set<IOperatorEventListener>>();
  
  public void register(OperatorID _id, IOperatorEventListener opEventListener)
  {
    if(!_listeners.containsKey(_id))
    {
      _listeners.put(_id, new HashSet<IOperatorEventListener>());
    }
    _listeners.get(_id).add(opEventListener);
  }
  
  public void raise(OperatorID _id)
  {
    if(!_listeners.containsKey(_id))
    {
      return;
    }
    else
    {
      for(IOperatorEventListener listener : _listeners.get(_id))
      {
        listener.notifyOnOperatorEvent(this);
      }
    }
  }
}
