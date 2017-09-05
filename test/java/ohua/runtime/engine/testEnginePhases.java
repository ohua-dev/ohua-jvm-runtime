/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.File;

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
public class testEnginePhases extends AbstractFlowTestCase
{
  private AbstractProcessManager loadProcess() throws Throwable
  {
    return loadProcess(getTestClassInputDirectory()
                        + "DeterministicMerge-correctness-Consumer-flow.xml");
  }

  private AbstractProcessManager loadProcess(File pathToRuntimeProperties) throws Throwable
  {
    return loadProcess(getTestClassInputDirectory()
                           + "DeterministicMerge-correctness-Consumer-flow.xml",
                       pathToRuntimeProperties.getAbsolutePath());
  }

  @Test(timeout = 8000)
  public void testInitPhase() throws Throwable
  {
    AbstractProcessManager manager = loadProcess();
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    
    performInitPhaseAssertions(manager);
  }

  @Test(timeout = 8000)
  public void testComputationPhase() throws Throwable
  {
    AbstractProcessManager manager = loadProcess();
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
  public void testComputationPhase2() throws Throwable
  {
    AbstractProcessManager manager = loadProcess(new File(getTestMethodInputDirectory() + "runtime-parameters.properties"));
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
