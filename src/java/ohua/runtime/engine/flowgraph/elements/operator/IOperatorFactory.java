/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;

public interface IOperatorFactory
{
  boolean exists(String operatorName);
  
  OperatorCore createUserOperatorCore(FlowGraph graph, String operatorName) throws OperatorLoadingException;
}
