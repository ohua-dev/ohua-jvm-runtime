/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.link.JavaBackendProvider;
import ohua.runtime.engine.flowgraph.elements.operator.Arc.ArcType;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.utils.GraphVisualizer;
import ohua.lang.Condition;
import ohua.runtime.test.AbstractFlowTestCase;
import ohua.util.Tuple;
import ohua.lang.defsfn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

public class testCycles extends AbstractFlowTestCase
{
  @Before
  public void clearLinker() {
    OperatorFactory.getInstance().clear();
  }
  
  /**
   * A simple cycle realized with a merge and a switch.
   *
   * FIXME add back whe loop support is on the table again
   */
  @Ignore
  @Test(timeout = 20000)
  public void testSimpleCycle() throws Throwable {
      registerFunc("func-prod", testFunctionalOperator.FunctionalProducer.class.getDeclaredMethod("produce"));
      registerFunc("inc", IncOperator.class.getDeclaredMethod("inc", int.class, String.class));
      registerFunc("func-cons", testFunctionalOperator.FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, testFunctionalOperator.ResultCapture.class));
      JavaBackendProvider.loadCoreOperators();
//    GraphVisualizer.PRINT_FLOW_GRAPH = getTestMethodOutputDirectory() + "flow-graph";

    OhuaRuntime runtime = new OhuaRuntime();
      createOp(runtime, "func-prod", 100);
      runtime.createOperator("com.ohua.lang/merge", 101);
      runtime.createOperator("com.ohua.lang/ifThenElse", 102);
      createOp(runtime, "inc", 103);
      createOp(runtime, "func-cons", 104);

    // prod -> nd-merge
    runtime.registerDependency(100, 0, 101, 0);
    runtime.registerDependency(100, 1, 101, 0);

    // nd-merge -> switch input
    runtime.registerDependency(101, 0, 102, 1);

    // switch result connections
    runtime.registerDependency(102, 0, 103, -1, ArcType.CYCLE_START.ordinal());
    runtime.registerDependency(102, 1, 104, -1);

    // nd-merge (loop-start) -> inc (if-branch)
    runtime.registerDependency(101, 0, 103, 0, ArcType.CYCLE_START.ordinal());
    runtime.registerDependency(101, 1, 103, 1, ArcType.CYCLE_START.ordinal());

      // inc -> nd-merge
    runtime.registerDependency(103, 0, 101, 1, ArcType.FEEDBACK_EDGE.ordinal());
    runtime.registerDependency(103, 1, 101, 1, ArcType.FEEDBACK_EDGE.ordinal());

      // nd-merge (loop-exit) -> (else-branch)
    runtime.registerDependency(101, 0, 104, 0);
    runtime.registerDependency(101, 1, 104, 1);

      runtime.setArguments(102, new Tuple[] { new Tuple(0, new Condition() {
      @Override
      public boolean check(Object[] args) {
        return ((int) args[0]) < 100;
      }
    } )});

      testFunctionalOperator.ResultCapture finalCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(104, new Tuple[] { new Tuple(2, finalCapture) });

      runtime.execute();

      Assert.assertEquals(105, finalCapture._iResult);
    Assert.assertEquals("some", finalCapture._sResult);
  }

    // FIXME put back once loops are back on the table
    @Ignore
  @Test(timeout = 20000)
  public void testImplicitFeedback() throws Throwable {
        registerFunc("func-prod", testFunctionalOperator.FunctionalGenericProducer.class.getDeclaredMethod("produce", Object[].class));
        registerFunc("inc", RawIncOperator.class.getDeclaredMethod("inc", int.class));
        Map ops = null;
    ops.put("func-prod", testFunctionalOperator.FunctionalGenericProducer.class.getName());
    ops.put("inc", RawIncOperator.class.getName());
    ops.put("func-cons", testFunctionalOperator.IntConsumer.class.getName());
    // FIXME
//    OperatorLibrary.registerOperators(ops, getTestMethodOutputDirectory() + "test-registry.xml");
    JavaSFNLinker.loadCoreOperators();
    // FIXME
//    JavaSFNLinker.loadAppOperators(getTestMethodOutputDirectory() + "test-registry.xml");
//    GraphVisualizer.PRINT_FLOW_GRAPH = getTestMethodOutputDirectory() + "flow-graph";

        OhuaRuntime runtime = new OhuaRuntime();
        createOp(runtime, "func-prod", 100);
    runtime.createOperator("merge", 101);
    runtime.createOperator("ifThenElse", 102);
    runtime.createOperator("inc", 103);
    runtime.createOperator("func-cons", 104);

        // prod -> nd-merge
    runtime.registerDependency(100, 0, 101, 0);

    // nd-merge -> switch input
    runtime.registerDependency(101, -1, 102, 1);

        // switch result connections
    runtime.registerDependency(102, 0, 103, -1, ArcType.CYCLE_START.ordinal());
    runtime.registerDependency(102, 1, 104, -1);

        // nd-merge (loop-start) -> inc (if-branch)
    runtime.registerDependency(101, 0, 103, 0, ArcType.CYCLE_START.ordinal());

    // inc -> nd-merge (implicit mapping from feedback arc)
    runtime.registerDependency(103, -1, 101, 1, ArcType.FEEDBACK_EDGE.ordinal());

        // nd-merge (loop-exit) -> (else-branch)
    runtime.registerDependency(101, -1, 104, 0);

    runtime.setArguments(100, new Tuple[] { new Tuple(0, new Object[]{ 5 }) });

    runtime.setArguments(102, new Tuple[] { new Tuple(0, new Condition() {
      @Override
      public boolean check(Object[] args) {
        return ((int) args[0]) < 100;
      }
    }) });

        testFunctionalOperator.ResultCapture finalCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(104, new Tuple[] { new Tuple(1, finalCapture) });

        runtime.execute();

        Assert.assertEquals(105, finalCapture._iResult);
  }

    public static class IncOperator {
        @defsfn
        public Object[] inc(int i, String s) {
            return new Object[]{i + 100,
                    s};
        }
    }

    public static class RawIncOperator {
        @defsfn
        public int inc(int i) {
            return i + 100;
        }
    }
}
