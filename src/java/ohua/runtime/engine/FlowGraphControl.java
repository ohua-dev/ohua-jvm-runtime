/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OhuaOperator;
import ohua.runtime.engine.points.VisitorFactory;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.ActivationMarkerVisitorMixin;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.DataPollOnBlockedInputPortSignalMixin;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.EndStreamSignalVisitorMixin;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.handers.ActivationMarkerHandler;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.handers.EndOfStreamPacketHandler;
import ohua.runtime.engine.operators.system.UserGraphEntranceOperator;

import java.util.Set;

/**
 * Initialization functionality for operators and arcs.
 * @author sertel
 *         
 */
public abstract class FlowGraphControl {

  /**
   * Calls the initialization routines of the operators and transitions their state to WAITING_FOR_COMPUTATION.
   * @param ops
   */
  public static void prepareAndEnterComputationState(Set<AbstractOperatorRuntime> ops) {
    // TODO could be done in parallel
    ops.stream().forEach(AbstractOperatorRuntime::prepareAndEnterComputationState);
  }

  public static void performPortHandlerSetup(OhuaOperator op, OperatorCore core, EndOfStreamPacketHandler endHandler) {
    // we need somebody that handles the data and somebody that handles end-of-stream signals
    ActivationMarkerHandler activationHandler = new ActivationMarkerHandler(op);

    for(InputPort inPort : op.getInputPorts()) {
      EndStreamSignalVisitorMixin endStreamPacketVisitor = VisitorFactory.createEndStreamPacketVisitor(inPort);
      endStreamPacketVisitor.registerMarkerHandler(endHandler);
      inPort.registerPacketVisitor(endStreamPacketVisitor);
      
      // we also want a DataPollOnBlockedInputPort visitor on all input ports
      DataPollOnBlockedInputPortSignalMixin dataPollVisitor =
          VisitorFactory.createDataPollOnBlockedPortEventVisitor(inPort);
      inPort.registerPacketVisitor(dataPollVisitor);
      
      if(op.getUserOperator() instanceof UserGraphEntranceOperator
         && !inPort.getPortName().equals("process-control"))
      {
        continue;
      }
      
      // stuff for user graph and input path to user graph (ProcessController, etc.)
      
      ActivationMarkerVisitorMixin activationPacketVisitor = VisitorFactory.createActivationMarkerVisitor(inPort);
      inPort.registerPacketVisitor(activationPacketVisitor);
      activationPacketVisitor.registerMarkerHandler(activationHandler);
      activationPacketVisitor.registerMarkerHandler(endHandler);
    }
  }

  public static void tearDownAndFinishComputation(Set<AbstractOperatorRuntime> ops){
    // TODO could be done in parallel
    ops.stream().forEach(AbstractOperatorRuntime::tearDownAndFinishComputation);
  }
}
