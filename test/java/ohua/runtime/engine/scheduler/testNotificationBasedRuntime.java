/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.AbstractProcessManager;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.operators.ConsumerOperator;
import ohua.runtime.engine.operators.GeneratorOperator;
import ohua.runtime.engine.sections.OneOpOneSectionGraphBuilder;
import ohua.runtime.engine.sections.SectionGraph;
import ohua.runtime.engine.testEnginePhasesMT;
import ohua.runtime.test.AbstractFlowTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class testNotificationBasedRuntime extends AbstractFlowTestCase {

  public static FlowGraph oneOpOneSectionBuilderSimpleCorrectnessFlow(int amount) throws Exception {
    FlowGraph graph = new FlowGraph();
    OperatorFactory operatorFactory = graph.getOperatorFactory();
    OperatorCore gen = operatorFactory.createUserOperatorCore(graph, "Generator");
    gen.setOperatorName("Left-DataGenerator");
    OperatorCore consumer = operatorFactory.createUserOperatorCore(graph, "Consumer");
    consumer.setOperatorName("Consumer");

    graph.addArc(new Arc(gen.getOutputPort("output"), consumer.getInputPort("input")));

    GeneratorOperator.GeneratorProperties props1 = new GeneratorOperator.GeneratorProperties();
    props1.setAmountToGenerate(amount);
    props1.setStartOffset(40);
    props1.setSchema(Collections.singletonList("test"));
    ((GeneratorOperator) gen.getOperatorAlgorithm()).setProperties(props1);

    return graph;
  }


  /**
   * Generator -> Consumer
   */
  @Test(timeout = 10000)
  public void testOneOpOneSectionBuilder1() throws Throwable
  {
    AbstractProcessManager manager =
        loadProcess(oneOpOneSectionBuilderSimpleCorrectnessFlow(2000));
    
    OneOpOneSectionGraphBuilder builder = new OneOpOneSectionGraphBuilder(null);
    SectionGraph sectionGraph = builder.build(manager.getProcess().getGraph());
    
    Assert.assertEquals(2, sectionGraph.getAllSections().size());
    Assert.assertEquals(2, sectionGraph.getAllOperators().size());
    Assert.assertEquals(1, sectionGraph.getInputSections().size());
    Assert.assertEquals(1, sectionGraph.getOutputSections().size());
  }
  
  /**
   * More complex flow with merge and split
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testOneOpOneSectionBuilder2() throws Throwable
  {
    AbstractProcessManager manager =
        loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"));
    
    OneOpOneSectionGraphBuilder builder = new OneOpOneSectionGraphBuilder(null);
    SectionGraph sectionGraph = builder.build(manager.getProcess().getGraph());
    
    Assert.assertEquals(11, sectionGraph.getAllSections().size());
    Assert.assertEquals(11, sectionGraph.getAllOperators().size());
    Assert.assertEquals(2, sectionGraph.getInputSections().size());
    Assert.assertEquals(2, sectionGraph.getOutputSections().size());
  }
  
  /**
   * Generator -> DatabaseWriter
   * @throws Throwable
   */
  @Test(timeout = 8000)
  public void testSimpleFlow() throws Throwable {
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE, RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE, 2);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY, 25);
    AbstractProcessManager manager =
            loadProcess(oneOpOneSectionBuilderSimpleCorrectnessFlow(100), config);
    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }
  
  /**
   * More complex flow with non-deterministic merge and split.<br/>
   * 2 threads
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testComplexFlow() throws Throwable {
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE, RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE, 2);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY, 25);
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"), config);
    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());

  }
  
  /**
   * More complex flow with deterministic merge and split.<br/>
   * 2 threads
   * @throws Throwable
   */
  @Test(timeout = 8000)
  public void testComplexFlow2() throws Throwable {
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE, RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE, 2);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY, 25);
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"), config);
    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }
  
  /**
   * More complex flow with non-deterministic merge and split.<br/>
   * More threads than operators.
   * @throws Throwable
   */
  @Test(timeout = 16000)
  public void testComplexFlow3() throws Throwable {
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE, RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE, 20);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY, 25);
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"), config);
    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * More complex flow with deterministic merge and split.<br/>
   * More threads than operators.
   * @throws Throwable
   */
  @Test(timeout = 16000)
  public void testComplexFlow4() throws Throwable {
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE, RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE, 20);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY, 25);
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"), config);
    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

}
