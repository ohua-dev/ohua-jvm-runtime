/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import java.util.List;

import ohua.util.Tuple;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.lang.operator.AbstractFunctionalOperator;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;

public abstract class OneToNSupport {

  public static class OneToNData extends Tuple<Integer, Object> {
    public OneToNData(Integer count, Object data) {
      super(count, data);
    }
  }

  public static class OneToN {
    @defsfn
    public OneToNData oneToN(int preserve, Object data) {
      /*
       * Note here that the second parameter essentially wraps the passed object into another array.
       * the algorithm expects to find this in the operators and just passes along the wrapped array as a normal
       * parameter array that would have been retrieved from a packet!
       */
      return new OneToNData(preserve, data );
    }
  }

  /**
   * Propagate the information for handling one-to-n transfers.
   *
   * @param graph
   */
  public static void prepare(FlowGraph graph) {
    for(OperatorCore pairFunc : graph.getOperators("com.ohua.lang/one-to-n.*")) {
      List<OutputPort> outPorts = pairFunc.getOutputPorts();
//      Assertion.invariant(outPorts.size() == 1, "Function '" + pairFunc.getOperatorName() + "' should have single output port but has: " + outPorts.size());
      outPorts.stream().forEach(op -> {
        InputPort targetPort = op.getOutgoingArcs().get(0).getTargetPort();
        ((AbstractFunctionalOperator) targetPort.getOwner().getOperatorAlgorithm()).addOutOfContextInput(targetPort.getPortName());
      });
//      InputPort targetPort = outPorts.get(0).getOutgoingArcs().get(0).getTargetPort();
//      ((AbstractFunctionalOperator) targetPort.getOwner().getOperatorAlgorithm()).addOutOfContextInput(targetPort.getPortName());
    }
  }

}
