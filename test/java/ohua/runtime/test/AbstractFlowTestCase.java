/*
 * Copyright (c) Sebastian Ertel 2008-2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.test;

import ohua.runtime.engine.*;
import ohua.runtime.engine.ProcessID.ProcessIDGenerator;
import ohua.runtime.engine.daapi.MapDataFormat;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorStateMachine.OperatorState;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractPort.PortState;
import ohua.runtime.engine.operators.*;
import ohua.runtime.engine.scheduler.AbstractScheduler;
import ohua.runtime.test.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;

import java.util.function.Supplier;

@Ignore
public abstract class AbstractFlowTestCase extends AbstractRegressionTestCase
{

  @Before
  public void loadTestOps(){
    OperatorFactory.registerUserOperator("Generator", GeneratorOperator.class, GeneratorOperator.description());
    OperatorFactory.registerUserOperator("Consumer", ConsumerOperator.class, ConsumerOperator.description());
    OperatorFactory.registerUserOperator("DeterministicMerge", DeterministicMergeOperator.class, DeterministicMergeOperator.description());
    OperatorFactory.registerUserOperator("NonDeterministicMerge", NonDeterministicMergeOperator.class, NonDeterministicMergeOperator.description());
    OperatorFactory.registerUserOperator("Peek", PeekOperator.class, PeekOperator.description());
    OperatorFactory.registerUserOperator("Split", SplitOperator.class, SplitOperator.description());
  }

  @Before
  public void resetProcessCounter()
  {
    ProcessIDGenerator.resetIDCounter();
  }

  protected ProcessRunner createProcessRunner(Supplier<DataFlowProcess> process) {
    return new ProcessRunner(process);
  }

  protected ProcessRunner createProcessRunner(Supplier<DataFlowProcess> process, RuntimeProcessConfiguration config) {
    return new ProcessRunner(process, config);
  }

  public final AbstractProcessManager loadProcess(FlowGraph graph) throws Throwable
  {
    ProcessRunner runner = createProcessRunner(() -> new DataFlowProcess().setGraph(graph), new RuntimeProcessConfiguration());
    runner.addProperty("data-format", new MapDataFormat());
    AbstractProcessManager processManager = runner.getProcessManager();
//    GraphVisualizer.PRINT_FLOW_GRAPH = getTestMethodOutputDirectory() + "process";
//    GraphVisualizer.printFlowGraph(processManager.getProcess().getGraph());
    return processManager;
  }

  public final AbstractProcessManager loadProcess(FlowGraph graph,
                                                  String pathToRuntimeProperties) throws Throwable
  {
    ProcessRunner runner = createProcessRunner(() -> new DataFlowProcess().setGraph(graph));
    runner.loadRuntimeConfiguration(pathToRuntimeProperties);
    runner.addProperty("data-format", new MapDataFormat());
    AbstractProcessManager processManager = runner.getProcessManager();
//    GraphVisualizer.PRINT_FLOW_GRAPH = getTestMethodOutputDirectory() + "process";
//    GraphVisualizer.printFlowGraph(processManager.getProcess().getGraph());
    return processManager;
  }

  public final AbstractProcessManager loadProcess(FlowGraph graph,
                                                  RuntimeProcessConfiguration config) throws Throwable
  {
    ProcessRunner runner = createProcessRunner(() -> new DataFlowProcess().setGraph(graph), config);
    runner.addProperty("data-format", new MapDataFormat());
    AbstractProcessManager processManager = runner.getProcessManager();
//    GraphVisualizer.PRINT_FLOW_GRAPH = getTestMethodOutputDirectory() + "process";
//    GraphVisualizer.printFlowGraph(processManager.getProcess().getGraph());
    return processManager;
  }

  public final void runFlowNoAssert(AbstractProcessManager manager) throws Throwable
  {
    ProcessRunner runner = new ProcessRunner(manager);
    try
    {
      runner.run();
    }
    catch(RuntimeException t)
    {
      if(t.getCause() != null)
      {
        throw t.getCause();
      }
      else
      {
        throw t;
      }
    }
    
  }

  protected void performDataPhaseAssertions(AbstractProcessManager manager)
  {
    performGeneralSystemPhaseAssertions(manager);
    
    // it is a data phase and there the ports must all be unblocked
    for(OperatorCore operator : manager.getProcess().getGraph().getContainedGraphNodes())
    {
      for(InputPort inPort : operator.getInputPorts())
      {
        Assert.assertTrue("Input port on operator " + operator.getID() + " has state "
                              + inPort.getState(),
                          inPort.getState() == PortState.NORMAL);
      }
      
      // every time we send an EOS downstream we also block the output port. therefore this
      // operators runProcessRoutine() is never gonna be called again for this system phase.
      for(OutputPort outPort : operator.getOutputPorts())
      {
        Assert.assertTrue("Output port on operator " + operator.getID() + " has state "
                          + outPort.getState(), outPort.getState() == PortState.BLOCKED);
      }
    }
  }

  protected final AbstractRuntime.RuntimeState getRuntimeState(AbstractProcessManager manager){
    AbstractRuntime runtime = TestUtil.getPrivateFieldReference(AbstractProcessManager.class, "_runtime", manager);
    AbstractScheduler scheduler = TestUtil.getPrivateFieldReference(AbstractRuntime.class, "_scheduler", runtime);
    AbstractRuntime.RuntimeState<? extends AbstractOperatorRuntime> runtimeState = TestUtil.getPrivateFieldReference(AbstractScheduler.class, "_runtimeState", scheduler);
    return runtimeState;
  }

  private OperatorState getOperatorState(AbstractProcessManager manager, OperatorCore operator){
    AbstractRuntime.RuntimeState<? extends AbstractOperatorRuntime> runtimeState = getRuntimeState(manager);
    return runtimeState._opRuntimes.get(operator).getOperatorState();
  }
  
  protected void performTeardownAssertions(AbstractProcessManager manager)
  {
    for(OperatorCore operator : manager.getProcess().getGraph().getContainedGraphNodes())
    {
      Assert.assertTrue("Operator " + operator.getID() + " has state "
                            + getOperatorState(manager, operator),
                        getOperatorState(manager, operator) == OperatorState.DONE);
    }
    
    // there must not be any packets on the stream left
    for(OperatorCore operator : manager.getProcess().getGraph().getContainedGraphNodes())
    {
      for(Arc arc : operator.getGraphNodeOutputConnections())
      {
        Assert.assertTrue(arc.isQueueEmpty());
      }
    }
    
    // it is a data phase and there the ports must all be unblocked
    for(OperatorCore operator : manager.getProcess().getGraph().getContainedGraphNodes())
    {
      for(InputPort inPort : operator.getInputPorts())
      {
        Assert.assertTrue("Input port on operator " + operator.getID() + " has state "
                              + inPort.getState(),
                          inPort.getState() == PortState.CLOSED);
      }
      
      for(OutputPort outPort : operator.getOutputPorts())
      {
        Assert.assertTrue("Output port on operator " + operator.getID() + " has state "
                          + outPort.getState(), outPort.getState() == PortState.CLOSED);
      }
    }
  }
  
  protected void performInitPhaseAssertions(AbstractProcessManager manager)
  {
    performGeneralSystemPhaseAssertions(manager);
    
    // initialization is a system phase and therefore the output ports must be blocked (they are
    // being unblocked again with the next activation marker)
    for(OperatorCore operator : manager.getProcess().getGraph().getContainedGraphNodes())
    {
      for(OutputPort outPort : operator.getOutputPorts())
      {
        Assert.assertTrue("Output port on operator " + operator.getID() + " has state "
                          + outPort.getState(), outPort.getState() == PortState.BLOCKED);
      }
    }
  }
  
  protected void performSystemPhaseAssertions(AbstractProcessManager manager)
  {
    performGeneralSystemPhaseAssertions(manager);
    checkAllOutputPorts(manager, PortState.BLOCKED);
  }
  
  protected final void checkAllOutputPorts(AbstractProcessManager manager, PortState state)
  {
    for(OperatorCore operator : manager.getProcess().getGraph().getContainedGraphNodes())
    {
      // every time we send an EOS downstream we also block the output port. therefore this
      // operators runProcessRoutine() is never gonna be called again for this system phase.
      for(OutputPort outPort : operator.getOutputPorts())
      {
        Assert.assertTrue("Output port on operator " + operator.getID() + " has state "
                          + outPort.getState(), outPort.getState() == state);
      }
    }
  }
  
  protected final void performGeneralSystemPhaseAssertions(AbstractProcessManager manager)
  {
    // all operators must be in the WAITING_FOR_DATA state
    for(OperatorCore operator : manager.getProcess().getGraph().getContainedGraphNodes())
    {
        Assert.assertTrue("Operator " + operator.getID() + " has state "
                        + getOperatorState(manager, operator),
                getOperatorState(manager, operator) == OperatorState.WAITING_FOR_COMPUTATION);
    }
    
    // there must not be any packets on the stream left
    for(OperatorCore operator : manager.getProcess().getGraph().getContainedGraphNodes())
    {
      for(Arc arc : operator.getGraphNodeOutputConnections())
      {
        Assert.assertTrue(printArcData(arc), arc.isQueueEmpty());
      }
    }
  }
  
  private String printArcData(Arc arc)
  {
    StringBuilder builder = new StringBuilder();
    builder.append("Packets in arc: ");
    while(!arc.isQueueEmpty())
    {
      builder.append(arc.getData().toString() + "\n");
    }
    
    return builder.toString();
  }
  
}
