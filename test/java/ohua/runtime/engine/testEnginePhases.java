/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.operators.AbstractMergeOperator;
import ohua.runtime.engine.operators.ConsumerOperator;
import ohua.runtime.engine.operators.GeneratorOperator;
import ohua.runtime.test.AbstractFlowTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Our flow-under-test will be the DeterministicMerge-correctness-flow because this turns out to
 * be one of the most challenging ones due to the port scheduling policies of the merge. Instead
 * of the database endpoint we use a consumer in order to make assertions easier.
 * @author sebastian
 * 
 */
public class testEnginePhases extends AbstractFlowTestCase
{

  public static FlowGraph deterministicMergeCorrectnessConsumerFlow() throws Exception {
    FlowGraph graph = new FlowGraph();
    OperatorFactory operatorFactory = graph.getOperatorFactory();
    OperatorCore gen1 = operatorFactory.createUserOperatorCore(graph, "Generator");
    gen1.setOperatorName("Left-DataGenerator");
    OperatorCore gen2 = operatorFactory.createUserOperatorCore(graph, "Generator");
    gen2.setOperatorName("Right-DataGenerator");
    OperatorCore merge = operatorFactory.createUserOperatorCore(graph, "DeterministicMerge");
    merge.setOperatorName("DataMerge");
    OperatorCore peek = operatorFactory.createUserOperatorCore(graph, "Peek");
    peek.setOperatorName("Logger");
    OperatorCore consumer = operatorFactory.createUserOperatorCore(graph, "Consumer");
    consumer.setOperatorName("Consumer");

    graph.addArc(new Arc(gen1.getOutputPort("output"), merge.getInputPort("input_1")));
    graph.addArc(new Arc(gen2.getOutputPort("output"), merge.getInputPort("input_2")));
    graph.addArc(new Arc(merge.getOutputPort("output"), peek.getInputPort("input")));
    graph.addArc(new Arc(peek.getOutputPort("output"), consumer.getInputPort("input")));

    GeneratorOperator.GeneratorProperties props1 = new GeneratorOperator.GeneratorProperties();
    props1.setAmountToGenerate(50);
    props1.setSchema(Collections.singletonList("test"));
    ((GeneratorOperator) gen1.getOperatorAlgorithm()).setProperties(props1);
    GeneratorOperator.GeneratorProperties props2 = new GeneratorOperator.GeneratorProperties();
    props2.setAmountToGenerate(50);
    props2.setStartOffset(50);
    props2.setSchema(Collections.singletonList("test"));
    ((GeneratorOperator) gen2.getOperatorAlgorithm()).setProperties(props2);
    AbstractMergeOperator.MergeOperatorProperties mergeProps = new AbstractMergeOperator.MergeOperatorProperties();
    mergeProps.setDequeueBatchSize(1);
    ((AbstractMergeOperator) merge.getOperatorAlgorithm()).setProperties(mergeProps);

    return graph;
  }

  @Test(timeout = 8000)
  public void testInitPhase() throws Throwable
  {
    AbstractProcessManager manager = loadProcess(deterministicMergeCorrectnessConsumerFlow());
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    
    performInitPhaseAssertions(manager);
  }

  @Test(timeout = 8000)
  public void testComputationPhase() throws Throwable
  {
    AbstractProcessManager manager = loadProcess(deterministicMergeCorrectnessConsumerFlow());
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    manager.runProcessStartUp();
    manager.runDataPhase();
    manager.awaitSystemPhaseCompletion();
    
    performDataPhaseAssertions(manager);
  }

  @Override
  protected void performDataPhaseAssertions(AbstractProcessManager manager)
  {
    super.performDataPhaseAssertions(manager);
    
    // the consumer should have caught 100 packets
    Assert.assertEquals(100,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }
  
  @Test(timeout = 8000)
  public void testTearDownPhase() throws Throwable
  {
    AbstractProcessManager manager = loadProcess(deterministicMergeCorrectnessConsumerFlow());
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    manager.runProcessStartUp();
    manager.runDataPhase();
    manager.awaitSystemPhaseCompletion();
    manager.tearDownProcess();
    manager.awaitSystemPhaseCompletion();
    
    performTeardownAssertions(manager);
  }

  @Override
  protected void performTeardownAssertions(AbstractProcessManager manager)
  {
    super.performTeardownAssertions(manager);
    
    // the consumer should have caught 100 packets
    Assert.assertEquals(100,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }
  
  /**
   * Here we should adjust the packets for the generator to be a multiple of the arc boundary.
   * What will happen is that the operator here will be blocked and the generator has to be
   * scheduled again by its downstream operator in order to close out on its computation.
   */
  @Test//(timeout = 8000)
  public void testComputationPhase2() throws Throwable {
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    config._properties.setProperty("arc-boundary", "25");
    AbstractProcessManager manager = loadProcess(deterministicMergeCorrectnessConsumerFlow(), config);
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();

    manager.runProcessStartUp();
    manager.runDataPhase();
    manager.awaitSystemPhaseCompletion();
    performDataPhaseAssertions(manager);
    
    manager.tearDownProcess(); 
    manager.awaitSystemPhaseCompletion();
    performTeardownAssertions(manager);
  }

  @Override
  protected void performSystemPhaseAssertions(AbstractProcessManager manager)
  {
    super.performSystemPhaseAssertions(manager);

    // no data was processed
    Assert.assertEquals(0,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }
  
}
