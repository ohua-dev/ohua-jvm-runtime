/*
 * Copyright (c) Sebastian Ertel 2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.test.util.parser;

import ohua.runtime.test.AbstractRegressionTestCase;
import org.junit.Assert;
import org.junit.Test;

import ohua.runtime.engine.AbstractProcessManager;
import ohua.runtime.engine.DataFlowProcess;
import ohua.runtime.engine.OhuaProcessManager;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.operators.ConsumerOperator;
import ohua.runtime.engine.operators.GeneratorOperator;
import ohua.runtime.engine.utils.parser.OhuaFlowParser;

public class testProcessParser extends AbstractRegressionTestCase
{
  @Test
  public void parseSimpleGeneratorConsumerProcess() throws Throwable {
    OhuaFlowParser ohuaProcessParser = new OhuaFlowParser(getTestMethodInputDirectory() + "SimpleTestFlow.xml");
    DataFlowProcess process = ohuaProcessParser.load();
    
    Assert.assertEquals(1, process.getGraph().getContainedArcs().size());
    Assert.assertEquals(2, process.getGraph().getContainedGraphNodes().size());
    Assert.assertTrue(process.getGraph().getOperator("TestGenerator").getOperatorAlgorithm() instanceof GeneratorOperator);
    Assert.assertNotNull(((GeneratorOperator) process.getGraph().getOperator("TestGenerator").getOperatorAlgorithm()).getProperties());
    Assert.assertEquals(2000,
                        ((GeneratorOperator) process.getGraph().getOperator("TestGenerator").getOperatorAlgorithm()).getProperties().getAmountToGenerate());
    Assert.assertEquals(40,
                        ((GeneratorOperator) process.getGraph().getOperator("TestGenerator").getOperatorAlgorithm()).getProperties().getStartOffset());
  }
  
  @Test
  public void deserializeAndRunSimpleGeneratorConsumerProcess() throws Throwable {
    OhuaFlowParser ohuaProcessParser = new OhuaFlowParser(getTestMethodInputDirectory() + "SimpleTestFlow.xml");
    DataFlowProcess process = ohuaProcessParser.load();

    AbstractProcessManager manager =
        new OhuaProcessManager(process, new RuntimeProcessConfiguration());
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    manager.runFlow();
    manager.awaitSystemPhaseCompletion();
    manager.tearDownProcess();
    manager.awaitSystemPhaseCompletion();
    
    Assert.assertEquals(2000,
                        ((ConsumerOperator) process.getGraph().getOperator("TestConsumer").getOperatorAlgorithm()).getSeenPackets());
  }

}
