/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

public interface IInternalProcessManager
{
  public void activate(OperatorID operatorID);
  
  public void activate(String operatorName);
}
