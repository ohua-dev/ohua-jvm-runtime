/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

public abstract class AbstractExternalActivator
{
  public static final class ManagerProxy
  {
    private IInternalProcessManager _manager = null;
    
    protected ManagerProxy(IInternalProcessManager manager)
    {
      _manager = manager;
    }
    
    protected void activate(OperatorID operatorID)
    {
      _manager.activate(operatorID);
    }
  }
  
  private ManagerProxy _proxy = null;
  
  private OperatorID _operatorID = null;
  
  protected AbstractExternalActivator(OperatorID operatorID, ManagerProxy manager)
  {
    _operatorID = operatorID;
    _proxy = manager;
  }
  
  /**
   * The activation of an operator is idempotent.
   */
  protected final void activateOperator()
  {
    _proxy.activate(_operatorID);
  }
  
  abstract public void listen();
  
  abstract public void unlisten();
}
