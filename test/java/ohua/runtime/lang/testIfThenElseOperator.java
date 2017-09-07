/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import clojure.lang.Compiler;
import ohua.link.JavaBackendProvider;
import ohua.runtime.test.AbstractFlowTestCase;
import ohua.util.Tuple;
import ohua.lang.Condition;
import ohua.lang.Either;
import ohua.lang.defsfn;
import ohua.runtime.lang.operator.DataflowFunction;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.utils.GraphVisualizer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class testIfThenElseOperator extends AbstractFlowTestCase
{
  @Before
  public void clearLinker() {
    OperatorFactory.getInstance().clear();
  }
  
  /**
   * (let [[one two three] (prod )] (if (< one 5) (cons-1 two three) (cons-2 two three)))
   *
   * @throws Throwable
   */
  @Test(timeout = 30000)
  public void testBasicIf() throws Throwable {
//    GraphVisualizer.PRINT_FLOW_GRAPH = super.getTestMethodOutputDirectory() + "graph";
    clearCache();
    registerFunc("func-prod", MultiProducer.class.getDeclaredMethod("produce", List.class));
    registerFunc("func-cons", testFunctionalOperator.FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, testFunctionalOperator.ResultCapture.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 100);
    runtime.createOperator("ohua.lang/ifThenElse", 102);
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
    runtime.setArguments(100, new Tuple[] { new Tuple(0, input)});

    testFunctionalOperator.ResultCapture ifCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(104, new Tuple[] { new Tuple(2, ifCapture) });

    testFunctionalOperator.ResultCapture elseCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(105, new Tuple[] { new Tuple(2, elseCapture) });

    runtime.execute();

    Assert.assertEquals(100, ifCapture._iResult);
    Assert.assertEquals("some", ifCapture._sResult);
  }
  
  @Test(timeout = 30000)
  public void testBasicElse() throws Throwable {
//    GraphVisualizer.PRINT_FLOW_GRAPH = super.getTestMethodOutputDirectory() + "graph";
    clearCache();
    registerFunc("func-prod", MultiProducer.class.getDeclaredMethod("produce", List.class));
    registerFunc("func-cons", testFunctionalOperator.FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, testFunctionalOperator.ResultCapture.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 100);
    runtime.createOperator("ohua.lang/ifThenElse", 102);
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
    input.add(new Object[] { 7,
                            200,
                            "and" });
    runtime.setArguments(100, new Tuple[] { new Tuple(0, input) });

    testFunctionalOperator.ResultCapture ifCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(104, new Tuple[] { new Tuple(2, ifCapture) });

    testFunctionalOperator.ResultCapture elseCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(105, new Tuple[] { new Tuple(2, elseCapture)});

    runtime.execute();

    Assert.assertEquals(200, elseCapture._iResult);
    Assert.assertEquals("and", elseCapture._sResult);
  }
  
  /**
   * (let [[one two three] (prod )] (if (< one 5) (cons-1 two three) (cons-2 two three)))
   *
   * @throws Throwable
   */
  @Test(timeout = 30000)
  public void testBasic() throws Throwable {
    clearCache();
    registerFunc("func-prod", MultiProducer.class.getDeclaredMethod("produce", List.class));
    registerFunc("func-cons", testFunctionalOperator.FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, testFunctionalOperator.ResultCapture.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 100);
    runtime.createOperator("ohua.lang/ifThenElse", 102);
    createOp(runtime, "func-cons", 104);
    createOp(runtime, "func-cons", 105);

    runtime.registerDependency(100, 1, 104, 0);
    runtime.registerDependency(100, 2, 104, 1);

    runtime.registerDependency(100, 1, 105, 0);
    runtime.registerDependency(100, 2, 105, 1);

    // condition input
    runtime.registerDependency(100, 0, 102, 1);

    // condition output
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
    input.add(new Object[] { 7,
                            200,
                            "and" });
    runtime.setArguments(100, new Tuple[] { new Tuple(0, input) });

    testFunctionalOperator.ResultCapture ifCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(104, new Tuple[] { new Tuple(2, ifCapture) });

    testFunctionalOperator.ResultCapture elseCapture = new testFunctionalOperator.ResultCapture();
    runtime.setArguments(105, new Tuple[] { new Tuple(2, elseCapture) });

    runtime.execute();

    Assert.assertEquals(100, ifCapture._iResult);
    Assert.assertEquals("some", ifCapture._sResult);
    Assert.assertEquals(200, elseCapture._iResult);
    Assert.assertEquals("and", elseCapture._sResult);
  }
  
  @Test(timeout = 30000)
  @Ignore // FIXME We dont have `merge` anymore
  public void testWithConsume() throws Throwable {
    clearCache();
    loadCoreOps();
    registerFunc("produce", ClojureTestOps.TestProduceOperator.class.getDeclaredMethod("produce"));
    registerFunc("add", ClojureTestOps.AddOperator.class.getDeclaredMethod("add", int.class, long.class));
    registerFunc("subtract", ClojureTestOps.SubtractOperator.class.getDeclaredMethod("subtract", int.class, long.class));

    JavaBackendProvider.registerFunction("ohua.tests.lang", "collect", ClojureTestOps.TestCollectOperator.class.getDeclaredMethod("collect", long.class, long[].class));

    OhuaRuntime runtime = new OhuaRuntime
            ();
    createOp(runtime, "produce", 100);
    runtime.createOperator("com.ohua.tests.lang/collect", 101);
    createOp(runtime, "merge", 102);
    runtime.createOperator("com.ohua.lang/ifThenElse", 103);
    createOp(runtime, "add", 105);
    createOp(runtime, "subtract", 106);
    runtime.registerDependency(100, -1, 103, 1);
    runtime.registerDependency(100, -1, 105, 0);
    runtime.registerDependency(100, -1, 106, 0);
    runtime.registerDependency(102, -1, 101, 0);
    runtime.registerDependency(105, -1, 102, 0);
    runtime.registerDependency(106, -1, 102, 1);
    runtime.registerDependency(103, 0, 105, -1);
    runtime.registerDependency(103, 1, 106, -1);

    long[] result = new long[10];
    runtime.setArguments(101, new Tuple[] { new Tuple(1, result) });
    runtime.setArguments(103, new Tuple[] { new Tuple(0, new Condition() {
      @Override
      public boolean check(Object[] args) {
        return ((int) args[0]) < 3;
      }
    }) });
    runtime.setArguments(105, new Tuple[] { new Tuple(1, 100) });
    runtime.setArguments(106, new Tuple[] { new Tuple(1, 3) });

    runtime.execute();

    long sum = 0;
    for(long r : result)
      sum += r;

    int expected = 100 + 101 + 102 + (3 - 3) + (4 - 3) + (5 - 3) + (6 - 3) + (7 - 3) + (8 - 3) + (9 - 3);
    Assert.assertEquals(expected, sum);
  }
  
  @Test(timeout=30000)
  public void testTwoConditionInputs() throws Throwable {
//    GraphVisualizer.PRINT_FLOW_GRAPH = super.getTestMethodOutputDirectory() + "graph";
    clearCache();
    loadCoreOps();
    registerFunc("produce", ClojureTestOps.TestProduceOperator.class.getDeclaredMethod("produce"));
    registerFunc("consume", ClojureTestOps.TestConsumeOperator.class.getDeclaredMethod("consume", int.class, int[].class));

    String code =
        "(doto (new ohua.runtime.lang.OhuaRuntime)"
                + "(.createOperator \"" + testNS + "/produce\" 100)"
                + "(.createOperator \"" + testNS + "/produce\" 101)"
                + "(.createOperator \"ohua.lang/ifThenElse\" 103)"
                + "(.createOperator \"" + testNS + "/consume\" 105)"
                + "(.createOperator \"" + testNS + "/consume\" 106)"
            + "(.registerDependency 100 -1 103 1)"
            + "(.registerDependency 100 -1 105 0)"
            + "(.registerDependency 101 -1 103 2)"
            + "(.registerDependency 101 -1 106 0)"
            + "(.registerDependency 103 0 105 -1)"
            + "(.registerDependency 103 1 106 -1)"
            + "(.setArguments 103 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 0) (clojure.core/reify ohua.lang.Condition (check [this args] (= (clojure.core/nth args 0) (clojure.core/nth args 1)))) ))))"
            // see below
//            + "(.setArguments 105 (clojure.core/into-array java.lang.Object (clojure.core/seq [if-result])))"
//            + "(.setArguments 106 (clojure.core/into-array java.lang.Object (clojure.core/seq [else-result])))"
            + ")";
    new clojure.lang.RT(); // needed by Clojure
    OhuaRuntime runtime = (OhuaRuntime) Compiler.load(new StringReader(code));
    int[] ifResult = new int[1];
    int[] elseResult = new int[1];
    runtime.setArguments(105, new Tuple[] { new Tuple(1, ifResult) });
    runtime.setArguments(106, new Tuple[] { new Tuple(1, elseResult) });
    runtime.execute();

    Assert.assertEquals(9, ifResult[0]);
    Assert.assertEquals(0, elseResult[0]);
  }

  public static class MultiProducer {
    @DataflowFunction
    @defsfn
    public Either.EitherObjectArrayOrFinish produce(List<Object[]> input) {
      return input.isEmpty() ?
              new Either.EitherObjectArrayOrFinish().add(DataflowFunction.Finish.DONE) :
              new Either.EitherObjectArrayOrFinish().add(input.remove(0));
    }
  }
  
}
