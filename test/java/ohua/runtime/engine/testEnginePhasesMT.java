/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import ohua.runtime.test.AbstractFlowTestCase;
import org.junit.Assert;

import org.junit.Test;

import ohua.runtime.engine.operators.ConsumerOperator;

/**
 * Our flow-under-test will be the DeterministicMerge-correctness-flow because this turns out to
 * be one of the most challenging ones due to the port scheduling policies of the merge. Instead
 * of the database endpoint we use a consumer in order to make assertions easier.
 * @author sebastian
 * 
 */
public class testEnginePhasesMT extends AbstractFlowTestCase {
  
  private AbstractProcessManager loadProcess() throws Throwable {
    return loadProcess(getTestClassInputDirectory() + "1-Op-1-Section-complex-correctness-flow-2.xml",
                       getTestClassInputDirectory() + "runtime-parameters.properties");
  }
  
  @Test(timeout = 8000)
  public void testInitPhaseST() throws Throwable {
    AbstractProcessManager manager =
        loadProcess(getTestClassInputDirectory() + "1-Op-1-Section-complex-correctness-flow-2.xml");
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    
    performInitPhaseAssertions(manager);
  }
  
  @Test(timeout = 8000)
  public void testInitPhase() throws Throwable {
    AbstractProcessManager manager = loadProcess();
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    
    performInitPhaseAssertions(manager);
  }
  
  @Test(timeout = 8000)
  public void testGraphAnalysisPhase() throws Throwable {
    AbstractProcessManager manager = loadProcess();
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    
    performSystemPhaseAssertions(manager);
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
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer-Output").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer-Output").getOperatorAlgorithm()).getSeenPackets());
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
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer-Output").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer-Output").getOperatorAlgorithm()).getSeenPackets());
  }
  
  @Override
  protected void performSystemPhaseAssertions(AbstractProcessManager manager) {
    super.performSystemPhaseAssertions(manager);
    
    // no data was processed
    // the consumer should have caught 100 packets
    Assert.assertEquals(0,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer-Output").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(0,
                        ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer-Output").getOperatorAlgorithm()).getSeenPackets());
  }
  
}
