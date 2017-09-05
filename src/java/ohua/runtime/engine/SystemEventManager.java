/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import ohua.runtime.engine.flowgraph.elements.operator.Operator;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

public class SystemEventManager
{
  /*
   * System events
   */
  private Map<EngineEvents, CopyOnWriteArrayList<ISystemEventListener>> _listeners =
      new HashMap<EngineEvents, CopyOnWriteArrayList<ISystemEventListener>>();
  
  private Map<EngineEvents, CopyOnWriteArrayList<ISystemEventListener>> _removeRequests =
      new HashMap<EngineEvents, CopyOnWriteArrayList<ISystemEventListener>>();
  
  /*
   * Operator events
   */
  //FIXME this is heavy duty on memory!
  private Map<OperatorEvents, ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>>> _opListeners =
      new HashMap<OperatorEvents, ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>>>();
  
  private Map<OperatorEvents, ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>>> _opRemoveRequests =
      new HashMap<OperatorEvents, ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>>>();
  

  
  public SystemEventManager()
  {
    init();
  }
  
  private void init()
  {
    // prefill both maps to avoid object overhead and unnecessary synchronization overhead.
    for(EngineEvents event : EngineEvents.values())
    {
      _listeners.put(event, new CopyOnWriteArrayList<ISystemEventListener>());
      _removeRequests.put(event, new CopyOnWriteArrayList<ISystemEventListener>());
    }
    
    for(OperatorEvents event : OperatorEvents.values())
    {
      _opListeners.put(event, new ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>>());
      _opRemoveRequests.put(event, new ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>>());
    }

  }
  
  public void registerForSystemEvent(EngineEvents event, ISystemEventListener listener)
  {
    CopyOnWriteArrayList<ISystemEventListener> listeners = _listeners.get(event);
    listeners.addIfAbsent(listener);
  }
  
  public void unregisterFromSystemEvent(EngineEvents event, ISystemEventListener listener)
  {
    _removeRequests.get(event).addIfAbsent(listener);
  }
  
  /**
   * To be called by the initiator of the system event!
   * @param event
   */
  public void systemEventNotification(EngineEvents event)
  {
    for(Map.Entry<EngineEvents, CopyOnWriteArrayList<ISystemEventListener>> removeRequest : _removeRequests.entrySet())
    {
      _listeners.get(removeRequest.getKey()).removeAll(removeRequest.getValue());
      _removeRequests.get(removeRequest.getKey()).clear();
    }
    
    if(!_listeners.containsKey(event))
    {
      return;
    }
    
    for(ISystemEventListener listener : _listeners.get(event))
    {
      listener.notifyOnEvent(event);
    }
  }
  
  public void registerForOperatorEvent(OperatorEvents event,
                                       Operator operator,
                                       IOperatorEventListener listener)
  {
    ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>> mapEntry =
        _opListeners.get(event);
    mapEntry.putIfAbsent(operator.getId(), new CopyOnWriteArraySet<IOperatorEventListener>());
    
    mapEntry.get(operator.getId()).add(listener);
  }
  
  // FIXME this is a memory issue because the maps are never removed again!
  public void unregisterFromOperatorEvent(OperatorEvents event,
                                          Operator operator,
                                          IOperatorEventListener listener)
  {
    ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>> mapEntry =
        _opRemoveRequests.get(event);
    mapEntry.putIfAbsent(operator.getId(), new CopyOnWriteArraySet<IOperatorEventListener>());
    
    mapEntry.get(operator.getId()).add(listener);
    
  }
  
  public void operatorEventNotification(Operator operator, OperatorEvents event)
  {
    for(Map.Entry<OperatorEvents, ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>>> removeRequest : _opRemoveRequests.entrySet())
    {
      ConcurrentHashMap<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>> opMap = _opListeners.get(removeRequest.getKey());
      for(Map.Entry<OperatorID, CopyOnWriteArraySet<IOperatorEventListener>> removeRequestOp : removeRequest.getValue().entrySet())
      {
        opMap.get(removeRequestOp.getKey()).removeAll(removeRequestOp.getValue());
      }
    }
    
    if(!_opListeners.get(event).containsKey(operator.getId()))
    {
      return;
    }
    
    for(IOperatorEventListener listener : _opListeners.get(event).get(operator.getId()))
    {
      listener.notifyOnOperatorEvent(event);
    }
  }
}
