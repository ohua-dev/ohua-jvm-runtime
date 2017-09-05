/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

public class OhuaProcessProxy implements IInternalProcessManager
{
  private IInternalProcessManager _ohuaProcess = null;
  
  public void activate(String operatorName)
  {
    _ohuaProcess.activate(operatorName);
  }
    
  public void activate(OperatorID operatorID)
  {
    _ohuaProcess.activate(operatorID);
  }
  
}
