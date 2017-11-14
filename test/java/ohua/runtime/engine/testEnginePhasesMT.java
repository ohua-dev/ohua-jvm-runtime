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
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;
import ohua.runtime.engine.operators.AbstractMergeOperator;
import ohua.runtime.engine.operators.ConsumerOperator;
import ohua.runtime.engine.operators.GeneratorOperator;
import ohua.runtime.engine.operators.SplitOperator;
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
public class testEnginePhasesMT extends AbstractFlowTestCase {

  public static FlowGraph oneOpOneSectionComplexCorrectnessFlow2(String mergeType) throws Exception {
    FlowGraph graph = new FlowGraph();
    OperatorFactory operatorFactory = graph.getOperatorFactory();
    OperatorCore gen1 = operatorFactory.createUserOperatorCore(graph, "Generator");
    gen1.setOperatorName("Left-DataGenerator");
    OperatorCore logger1 = operatorFactory.createUserOperatorCore(graph, "Peek");
    logger1.setOperatorName("Left-Input-Logger");
    OperatorCore gen2 = operatorFactory.createUserOperatorCore(graph, "Generator");
    gen2.setOperatorName("Right-DataGenerator");
    OperatorCore logger2 = operatorFactory.createUserOperatorCore(graph, "Peek");
    logger2.setOperatorName("Right-Input-Logger");
    OperatorCore merge = operatorFactory.createUserOperatorCore(graph, mergeType);
    merge.setOperatorName("DataMerge");
    OperatorCore logger3 = operatorFactory.createUserOperatorCore(graph, "Peek");
    logger3.setOperatorName("Logger");
    OperatorCore split = operatorFactory.createUserOperatorCore(graph, "Split");
    split.addOutputPort(new OutputPort(split, "output_1"));
    split.addOutputPort(new OutputPort(split, "output_2"));
    split.setOperatorName("DataSplit");
    OperatorCore leftOutLogger = operatorFactory.createUserOperatorCore(graph, "Peek");
    leftOutLogger.setOperatorName("Left-Output-Logger");
    OperatorCore leftConsumer = operatorFactory.createUserOperatorCore(graph, "Consumer");
    leftConsumer.setOperatorName("Left-Consumer");
    OperatorCore rightOutLogger = operatorFactory.createUserOperatorCore(graph, "Peek");
    rightOutLogger.setOperatorName("Right-Output-Logger");
    OperatorCore rightConsumer = operatorFactory.createUserOperatorCore(graph, "Consumer");
    rightConsumer.setOperatorName("Right-Consumer");

    graph.addArc(new Arc(gen1.getOutputPort("output"), logger1.getInputPort("input")));
    graph.addArc(new Arc(gen2.getOutputPort("output"), logger2.getInputPort("input")));
    graph.addArc(new Arc(logger1.getOutputPort("output"), merge.getInputPort("input_1")));
    graph.addArc(new Arc(logger2.getOutputPort("output"), merge.getInputPort("input_2")));
    graph.addArc(new Arc(merge.getOutputPort("output"), logger3.getInputPort("input")));
    graph.addArc(new Arc(logger3.getOutputPort("output"), split.getInputPort("input")));
    graph.addArc(new Arc(split.getOutputPort("output_1"), leftOutLogger.getInputPort("input")));
    graph.addArc(new Arc(split.getOutputPort("output_2"), rightOutLogger.getInputPort("input")));
    graph.addArc(new Arc(leftOutLogger.getOutputPort("output"), leftConsumer.getInputPort("input")));
    graph.addArc(new Arc(rightOutLogger.getOutputPort("output"), rightConsumer.getInputPort("input")));

    GeneratorOperator.GeneratorProperties props1 = new GeneratorOperator.GeneratorProperties();
    props1.setAmountToGenerate(100);
    props1.setSchema(Collections.singletonList("test"));
    ((GeneratorOperator) gen1.getOperatorAlgorithm()).setProperties(props1);
    GeneratorOperator.GeneratorProperties props2 = new GeneratorOperator.GeneratorProperties();
    props2.setAmountToGenerate(100);
    props2.setStartOffset(100);
    props2.setSchema(Collections.singletonList("test"));
    ((GeneratorOperator) gen2.getOperatorAlgorithm()).setProperties(props2);
    AbstractMergeOperator.MergeOperatorProperties mergeProps = new AbstractMergeOperator.MergeOperatorProperties();
    mergeProps.setDequeueBatchSize(12);
    ((AbstractMergeOperator) merge.getOperatorAlgorithm()).setProperties(mergeProps);
    ((SplitOperator)split.getOperatorAlgorithm()).getProperties().setSchedulingInterval(10);

    return graph;
  }

  private AbstractProcessManager loadProcess() throws Throwable {
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE, RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE, 2);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY, 25);
    return loadProcess(oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"), config);
  }

  @Test(timeout = 8000)
  public void testInitPhase() throws Throwable {
    AbstractProcessManager manager = loadProcess();
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    
    performInitPhaseAssertions(manager);
  }
  

  @Test(timeout = 8000)
  public void testComputationPhase() throws Throwable {
    AbstractProcessManager manager = loadProcess();
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    manager.runProcessStartUp();
    manager.runDataPhase();
    manager.awaitSystemPhaseCompletion();
    
    performDataPhaseAssertions(manager);
    
  }
  
  @Override
  protected void performDataPhaseAssertions(AbstractProcessManager manager) {
    super.performDataPhaseAssertions(manager);
    
    // the consumer should have caught 100 packets
    Assert.assertEquals(100,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }
  
  @Test(timeout = 8000)
  public void testTearDownPhase() throws Throwable {
    AbstractProcessManager manager = loadProcess();
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
  protected void performTeardownAssertions(AbstractProcessManager manager) {
    super.performTeardownAssertions(manager);
    
    // the consumer should have caught 100 packets
    Assert.assertEquals(100,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }
  
  @Override
  protected void performSystemPhaseAssertions(AbstractProcessManager manager) {
    super.performSystemPhaseAssertions(manager);
    
    // no data was processed
    // the consumer should have caught 100 packets
    Assert.assertEquals(0,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(0,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }
  
}
