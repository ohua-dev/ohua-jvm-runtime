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

import java.util.function.Supplier;

// TODO all below elements should probably not be public
public abstract class DataFlowComposition implements Supplier<DataFlowProcess>
{
  protected final DataFlowProcess createProcess() {
    return new DataFlowProcess();
  }
  
  protected final FlowGraph createGraph() {
    return new FlowGraph();
  }
  
  protected final OperatorCore loadOperator(String operatorType, FlowGraph graph) throws OperatorLoadingException {
    String operatorName = operatorType;
    IOperatorFactory opFactory = graph.getOperatorFactory();
    if(!opFactory.exists(operatorName)) {
      throw new OperatorLoadingException("Not linked: '" + operatorName + "'");
    }
    return opFactory.createUserOperatorCore(graph, operatorName);
  }

  abstract protected IOperatorFactory operatorFactory();
  
  protected final Arc createArc() {
    return new Arc();
  }

  protected abstract OperatorCore findOperator(FlowGraph graph, int id);
  
  public abstract DataFlowProcess load() throws Exception;

  public DataFlowProcess get(){
    try{
      return load();
    }catch(Exception e){
      throw new RuntimeException(e);
    }
  }

}
