/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.operators.ConsumerOperator;
import ohua.runtime.test.AbstractFlowTestCase;
import org.junit.Assert;

import org.junit.Test;

import ohua.runtime.engine.AbstractProcessManager;
import ohua.runtime.engine.sections.OneOpOneSectionGraphBuilder;
import ohua.runtime.engine.sections.SectionGraph;

public class testMultiThreadingFramework extends AbstractFlowTestCase
{
  /**
   * Generator -> Consumer
   */
  @Test(timeout = 10000)
  public void testOneOpOneSectionBuilder1() throws Throwable
  {
    AbstractProcessManager manager =
        loadProcess(getTestMethodInputDirectory()
                    + "1-Op-1-Section-Builder-simple-correctness-flow.xml");
    
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
        loadProcess(getTestMethodInputDirectory()
                    + "1-Op-1-Section-Builder-complex-correctness-flow.xml");
    
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
  public void testSimpleFlow() throws Throwable
  {
    AbstractProcessManager manager =
            loadProcess(getTestMethodInputDirectory() + "1-Op-1-Section-simple-correctness-flow.xml",
                    getTestMethodInputDirectory() + "runtime-parameters.properties");
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
  public void testComplexFlow() throws Throwable
  {
    AbstractProcessManager manager =
            loadProcess(getTestMethodInputDirectory() + "1-Op-1-Section-complex-correctness-flow.xml",
                    getTestMethodInputDirectory() + "runtime-parameters.properties");
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
  public void testComplexFlow2() throws Throwable
  {
    AbstractProcessManager manager =
            loadProcess(getTestMethodInputDirectory() + "1-Op-1-Section-complex-correctness-flow-2.xml",
                    getTestMethodInputDirectory() + "runtime-parameters.properties");
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
  public void testComplexFlow3() throws Throwable
  {
    AbstractProcessManager manager =
            loadProcess(getTestMethodInputDirectory() + "1-Op-1-Section-complex-correctness-flow.xml",
                    getTestMethodInputDirectory() + "runtime-parameters.properties");
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
  public void testComplexFlow4() throws Throwable
  {
    AbstractProcessManager manager =
            loadProcess(getTestMethodInputDirectory() + "1-Op-1-Section-complex-correctness-flow-2.xml",
                    getTestMethodInputDirectory() + "runtime-parameters.properties");
    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

}
