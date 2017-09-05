/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.test.AbstractFlowTestCase;
import ohua.util.Tuple;
import ohua.lang.Condition;
import ohua.runtime.engine.AbstractProcessManager;
import ohua.runtime.engine.DataFlowProcess;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.RuntimeProcessConfiguration.Parallelism;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

@Ignore
public class testSectionMapping extends AbstractFlowTestCase {
  
  private AbstractProcessManager _manager = null;

  /**
   * This test barely assures that the runtime works with the new section mapper and gives an
   * example on how to pass section restrictions to the runtime. Another test assures that this
   * section mapper actually does the right thing.<br>
   * (Test code taken from basic if statement test.)
   */
  @Test(timeout = 30000)
  public void testBasicRestricted() throws Throwable {
    clearCache();
    loadCoreOps();
    registerFunc("func-prod", testIfThenElseOperator.MultiProducer.class.getDeclaredMethod("produce", List.class));
    registerFunc("func-cons", testFunctionalOperator.FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, testFunctionalOperator.ResultCapture.class));

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 100);
    runtime.createOperator("com.ohua.lang/ifThenElse", 102);
    createOp(runtime, "func-cons", 104);
    createOp(runtime, "func-cons", 105);

    runtime.registerDependency(100, 0, 102, 1);
    runtime.registerDependency(100, 1, 104, 0);
    runtime.registerDependency(100, 1, 105, 0);
    runtime.registerDependency(100, 2, 104, 1);
    runtime.registerDependency(100, 2, 105, 1);
    runtime.registerDependency(102, 0, 104, -1);
    runtime.registerDependency(102, 1, 105, -1);

    runtime.setArguments(102, new Tuple[] { new Tuple(0, new Condition() {
      @Override
      public boolean check(Object[] args) {
        return ((int) args[0]) < 5;
      }
    })});

    List<Object[]> input = new ArrayList<>();
    input.add(new Object[] { 3,
                             100,
                             "some" });
    runtime.setArguments(100, new Tuple[] { new Tuple(0, input) });

    testFunctionalOperator.ResultCapture ifCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(104, new Tuple[] { new Tuple(2, ifCapture) });

    testFunctionalOperator.ResultCapture elseCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(105, new Tuple[] { new Tuple(2, elseCapture) });

    // restrict the execution of the consumers to the same section.
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    Properties props = new Properties();
    props.setProperty("execution-mode", Parallelism.MULTI_THREADED.name());
    List<List<String>> sections = new ArrayList<>();
    sections.add(Collections.singletonList(testNS + "/func-cons.*"));
    props.put("section-config", sections);
    config.setProperties(props);
    // FIXME
//    PreparedRuntimeConfiguration prepConf =
//        new TestRuntimeConfig(runtime.getCompileTimeView(),
//                              runtime.getRuntimeView(),
//                              Collections.singletonMap(FlowGraphCompiler.SHARED_VARIABLES_INFO,
//                                                       Collections.emptyMap()));
//    config.aquirePropertiesAccess(prepConf);
//    runtime.execute(prepConf);

    // check that the proper sections got created.
    Assert.assertEquals(3, super.getRuntimeState(_manager)._sectionGraph.getAllSections().size());

    Assert.assertEquals(100, ifCapture._iResult);
    Assert.assertEquals("some", ifCapture._sResult);
  }

  private class TestRuntimeConfig extends PreparedRuntimeConfiguration {
    public TestRuntimeConfig(CompileTimeView cv, RuntimeView rv, Map<String, Object> compileTimeInfo) {
      super(cv, rv, compileTimeInfo);
    }

    public AbstractProcessManager getProcessManager(DataFlowProcess process) {
      _manager = super.getProcessManager(process);
      return _manager;
    }
  }

}
