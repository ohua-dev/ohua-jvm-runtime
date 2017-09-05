/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.sections;

import ohua.runtime.engine.flowgraph.elements.operator.AsynchronousArcImpl;
import junit.framework.Assert;

import ohua.runtime.test.AbstractFlowTestCase;
import org.junit.Ignore;
import org.junit.Test;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.operators.ConsumerOperator;
import ohua.runtime.engine.utils.GraphVisualizer;

@Ignore // I'm not sure anymore what these tests are really testing. Delete?!
public class testAdvancedSections extends AbstractFlowTestCase
{
  /**
   * Tests a section configuration where there exists a pipeline of 6 operators. Operator 1 is
   * on its own section and operator 4 as well. All other operators are located together on a
   * separate section.
   */
  @Test
  public void testUnconnectedSubflows() throws Throwable
  {
    FlowGraph graph =
        runFlowGetGraph(getTestMethodInputDirectory() + "flow.xml",
                        getTestMethodInputDirectory() + "runtime.properties");
    ConsumerOperator consumer = (ConsumerOperator) graph.getOperator("Request-Respond").getOperatorAlgorithm();
    Assert.assertEquals(5000, consumer.getSeenPackets());
  }
  
  /**
   * This is the very same test flow as above but put in a low latency configuration.
   * @throws Throwable
   */
  @Test
  public void testSectionCycleLowLatency() throws Throwable
  {
    AsynchronousArcImpl.ACTIVATION_MARK = 0;
    FlowGraph graph =
        runFlowGetGraph(getTestMethodInputDirectory() + "flow.xml",
                        getTestMethodInputDirectory() + "runtime.properties");
    ConsumerOperator consumer = (ConsumerOperator) graph.getOperator("Request-Respond").getOperatorAlgorithm();
    Assert.assertEquals(5000, consumer.getSeenPackets());    
  }
  
  /**
   * Same as the above but with data parallelism on the section cycle.
   * @throws Throwable
   */
  @Ignore // FIXME enable again once cycle are back on the plan
  @Test
  public void testSectionCycleParallel() throws Throwable
  {
    AsynchronousArcImpl.ACTIVATION_MARK = 0;
    GraphVisualizer.PRINT_SECTION_GRAPH = getTestMethodOutputDirectory() + "sections";
    FlowGraph graph =
        runFlowGetGraph(getTestMethodInputDirectory() + "flow.xml",
                        getTestMethodInputDirectory() + "runtime.properties");
    int seen = 0;
    seen += ((ConsumerOperator) graph.getOperator("Request-Respond").getOperatorAlgorithm()).getSeenPackets();
    seen += ((ConsumerOperator) graph.getOperator("Request-Respond-0").getOperatorAlgorithm()).getSeenPackets();
    seen += ((ConsumerOperator) graph.getOperator("Request-Respond-1").getOperatorAlgorithm()).getSeenPackets();
    Assert.assertEquals(5000, seen);    
  }
}
