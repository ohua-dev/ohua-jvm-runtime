/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph;

import ohua.runtime.engine.DataFlowProcess;
import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.IOperatorFactory;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;

// TODO all below elements should probably not be public
public abstract class DataFlowComposition
{
  protected final DataFlowProcess createProcess() {
    return new DataFlowProcess();
  }
  
  protected final FlowGraph createGraph() {
    return new FlowGraph();
  }
  
  protected final OperatorCore loadOperator(String operatorType, FlowGraph graph) throws OperatorLoadingException {
    String operatorName = operatorType;
    IOperatorFactory opFactory = operatorFactory();
    if(!opFactory.exists(operatorName)) {
      operatorName = convertOperatorName(operatorName);
    }
    return opFactory.createUserOperatorCore(graph, operatorName);
  }
  
  protected IOperatorFactory operatorFactory(){
    return OperatorFactory.getInstance();
  }
  
  protected final Arc createArc() {
    return new Arc();
  }

  protected abstract OperatorCore findOperator(FlowGraph graph, int id);
  
  public abstract DataFlowProcess load() throws Exception;
  
  /**
   * Called whenever an operator with such a name does not exist. The according subclass can use
   * this as a hook for example to convert between different naming conventions (camel case vs.
   * underscore).
   * @param operatorName
   * @return
   */
  protected String convertOperatorName(String operatorName) {
    return operatorName;
  }
  
}
