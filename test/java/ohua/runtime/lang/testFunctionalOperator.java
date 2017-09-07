/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.lang.defsfn;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.utils.GraphVisualizer;
import ohua.runtime.test.AbstractFlowTestCase;
import ohua.util.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class testFunctionalOperator extends AbstractFlowTestCase {
  @Before
  public void clearLinker() {
    OperatorFactory.getInstance().clear();
  }

  /**
   * A simple pipeline that matches two operators: a producer and a consumer.
   */
  @Test(timeout = 20000)
  public void testImplicitSchemaMatching() throws Throwable {
    registerFunc("func-prod", FunctionalProducer.class.getDeclaredMethod("produce"));
    registerFunc("func-cons", FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, ResultCapture.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 1);
    createOp(runtime, "func-cons", 2);

    runtime.createArc(1, 2);
    runtime.registerDependency(1, 0, 2, 0);
    runtime.registerDependency(1, 1, 2, 1);
    ResultCapture capture = new ResultCapture();
    runtime.setArguments(2, new Tuple[]{new Tuple(2, capture)});

    runtime.execute();

    Assert.assertEquals(5, capture._iResult);
    Assert.assertEquals("some", capture._sResult);
  }

  /**
   * Two producers, a merge and a consumer. This also shows that we can cope with inheritance in
   * the functional operator.
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  @Ignore
  // TODO @sertel please have a look at what invariant is broken here, maybe you can figure it out
  public void testImplicitMatchMultipleInputs() throws Throwable {
    clearCache();
    registerFunc("func-prod", FunctionalProducer.class.getDeclaredMethod("produce"));
    registerFunc("func-cons", FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, ResultCapture.class));
    registerFunc("func-merge", FunctionalMerge.class.getDeclaredMethod("merge", int.class, String.class, int.class, String.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 1);
    createOp(runtime, "func-prod", 2);
    createOp(runtime, "func-merge", 3);
    createOp(runtime, "func-cons", 4);

    runtime.createArc(1, 3);
    runtime.registerDependency(1, 0, 3, 0);
    runtime.registerDependency(1, 1, 3, 1);
    runtime.createArc(2, 3);
    runtime.registerDependency(2, 0, 3, 2);
    runtime.registerDependency(2, 1, 3, 3);
    runtime.createArc(3, 4);
    runtime.registerDependency(3, 0, 4, 0);
    runtime.registerDependency(3, 1, 4, 1);
    runtime.registerDependency(3, 2, 4, 2);
    runtime.registerDependency(3, 3, 4, 3);
    MergeResultCapture capture = new MergeResultCapture();
    runtime.setArguments(4, new Tuple[]{new Tuple(4, capture)});

    runtime.execute();

    Assert.assertEquals(5, capture._iResult);
    Assert.assertEquals("some", capture._sResult);
    Assert.assertEquals(5, capture._jResult);
    Assert.assertEquals("some", capture._tResult);
  }

  @Test
  public void testImplicitMatchMultipleOutputs() throws Throwable {
    /*
     * There is no such thing as implicit matching of multi-outputs. Whether an operator has one
     * or multiple outputs is now solely define by the algorithm. The algorithm decides where
     * each value of an output record should be input to. Hence, it defines this explicitly. On
     * the other hand, operators that either introduce data parallelism by a certain strategy
     * (switch, split, etc.) are system unique operators and therefore are not part of the
     * operator author space.
     */
  }

  /**
   * Clojure code:<br>
   * (let [[int-out string-out1] (peek (prod)] (int-consume int-out) (string-consume
   * string-out))<br>
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  public void testExplicitSchemaMatching() throws Throwable {
    clearCache();
    registerFunc("func-prod", FunctionalProducer.class.getDeclaredMethod("produce"));
    registerFunc("func-peek", FunctionalPeek.class.getDeclaredMethod("peek", int.class, String.class));
    registerFunc("int-cons", IntConsumer.class.getDeclaredMethod("consume", int.class, ResultCapture.class));
    registerFunc("string-cons", StringConsumer.class.getDeclaredMethod("consume", String.class, ResultCapture.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 1);
    createOp(runtime, "func-peek", 2);
    createOp(runtime, "int-cons", 3);
    createOp(runtime, "string-cons", 4);

    // explicit schema matching: the output of the peek is piped to both consumers
    runtime.registerDependency(1, 0, 2, 0);
    runtime.registerDependency(1, 1, 2, 1);
    runtime.registerDependency(2, 0, 3, 0);
    runtime.registerDependency(2, 1, 4, 0);

    ResultCapture capture = new ResultCapture();
    runtime.setArguments(3, new Tuple[]{new Tuple(1, capture)});
    runtime.setArguments(4, new Tuple[]{new Tuple(1, capture)});

    runtime.execute();

    Assert.assertEquals(5, capture._iResult);
    Assert.assertEquals("some", capture._sResult);
  }

  /**
   * Clojure code: (let[[one two _] (accept "input")] (read one) (write two))
   */
  @Test(timeout = 20000)
  public void testExplicitSchemaMatchOne() throws Throwable {
//    GraphVisualizer.PRINT_FLOW_GRAPH = getTestMethodOutputDirectory() + "process";

    clearCache();
    registerFunc("func-prod", FunctionalProducer.class.getDeclaredMethod("produce"));
    registerFunc("int-cons", IntConsumer.class.getDeclaredMethod("consume", int.class, ResultCapture.class));
    registerFunc("string-cons", StringConsumer.class.getDeclaredMethod("consume", String.class, ResultCapture.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 638);
    createOp(runtime, "int-cons", 1483);
    createOp(runtime, "string-cons", 704);
    runtime.registerDependency(638, 0, 1483, 0);
    runtime.registerDependency(638, 1, 704, 0);

    ResultCapture capture = new ResultCapture();
    runtime.setArguments(1483, new Tuple[]{new Tuple(1, capture)});
    runtime.setArguments(704, new Tuple[]{new Tuple(1, capture)});

    runtime.execute();

    Assert.assertEquals(5, capture._iResult);
    Assert.assertEquals("some", capture._sResult);
  }

  /**
   * Clojure code: (let[[one two _] (accept "input")] (read two) (write one))<br>
   * (Switched usage of the output parameters.)
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  public void testExplicitSchemaMatchTwo() throws Throwable {
    clearCache();
    registerFunc("func-prod", FunctionalProducer.class.getDeclaredMethod("produce"));
    registerFunc("int-cons", IntConsumer.class.getDeclaredMethod("consume", int.class, ResultCapture.class));
    registerFunc("string-cons", StringConsumer.class.getDeclaredMethod("consume", String.class, ResultCapture.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 1945);
    createOp(runtime, "string-cons", 4015);
    createOp(runtime, "int-cons", 7644);
    runtime.registerDependency(1945, 0, 7644, 0);
    runtime.registerDependency(1945, 1, 4015, 0);

    ResultCapture capture = new ResultCapture();
    runtime.setArguments(4015, new Tuple[]{new Tuple(1, capture)});
    runtime.setArguments(7644, new Tuple[]{new Tuple(1, capture)});

    runtime.execute();

    Assert.assertEquals(5, capture._iResult);
    Assert.assertEquals("some", capture._sResult);
  }

  /**
   * Clojure code: (let[[one two three] (accept "input")] (read two) (write one three)) <br>
   * (Switched usage but two non-consecutive args are piped to one downstream.)
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  @Ignore
  // TODO @sertel please have a look at what invariant is broken here, maybe you can figure it out
  public void testExplicitSchemaMatchThree() throws Throwable {
//    GraphVisualizer.PRINT_FLOW_GRAPH = getTestMethodOutputDirectory() + "process";

    clearCache();
    registerFunc("func-prod", FunctionalProducer.class.getDeclaredMethod("produce"));
    registerFunc("string-cons", StringConsumer.class.getDeclaredMethod("consume", String.class, ResultCapture.class));
    registerFunc("func-cons", FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, ResultCapture.class));
    loadCoreOps();

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 8313);
    createOp(runtime, "string-cons", 9801);
    createOp(runtime, "func-cons", 4232);
    runtime.registerDependency(8313, 0, 4232, 0);
    runtime.registerDependency(8313, 1, 9801, 0);
    runtime.registerDependency(8313, 2, 4232, 1);

    runtime.setArguments(8313, new Tuple[]{new Tuple(0, new Object[]{5,
            "some",
            "and"})});
    ResultCapture capture = new ResultCapture();
    runtime.setArguments(9801, new Tuple[]{new Tuple(1, capture)});
    ResultCapture funcCapture = new ResultCapture();
    runtime.setArguments(4232, new Tuple[]{new Tuple(2, funcCapture)});

    runtime.execute();

    Assert.assertEquals(5, funcCapture._iResult);
    Assert.assertEquals("and", funcCapture._sResult);
    Assert.assertEquals("some", capture._sResult);
  }

  /**
   * Clojure code: (let[[one two three] (accept "input")] (write one (read "something") three))<br>
   * (Multiple partial outputs and one set of implicit outputs. -> tests the destructuring.)
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  public void testExplicitSchemaMatchFour() throws Throwable {
//    GraphVisualizer.PRINT_FLOW_GRAPH = getTestMethodOutputDirectory() + "process";

    clearCache();
    loadCoreOps();
    registerFunc("func-cons", FunctionalMergeConsumer.class.getDeclaredMethod("consume", int.class, String.class, int.class, String.class, MergeResultCapture.class));
    registerFunc("func-gen-prod", FunctionalGenericProducer.class.getDeclaredMethod("produce", Object[].class));

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-gen-prod", 9728);
    createOp(runtime, "func-cons", 208);
    createOp(runtime, "func-gen-prod", 4101);
    runtime.registerDependency(9728, 0, 208, 0);
    runtime.registerDependency(4101, 0, 208, 1);
    runtime.registerDependency(4101, 1, 208, 2);
    runtime.registerDependency(9728, 2, 208, 3);

    runtime.setArguments(4101, new Tuple[]{new Tuple(0, new Object[]{"and",
            6})});
    runtime.setArguments(9728, new Tuple[]{new Tuple(0, new Object[]{5,
            "unused",
            "some"})});

    MergeResultCapture capture = new MergeResultCapture();
    runtime.setArguments(208, new Tuple[]{new Tuple(4, capture)});

    runtime.execute();

    Assert.assertEquals(5, capture._iResult);
    Assert.assertEquals("and", capture._sResult);
    Assert.assertEquals(6, capture._jResult);
    Assert.assertEquals("some", capture._tResult);
  }

  /**
   * (let [[one two] (prod )] (cons two one))
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  public void testTwistedExcplicitSchemaMatch() throws Throwable {
    clearCache();
    loadCoreOps();
    registerFunc("func-prod", FunctionalGenericProducer.class.getDeclaredMethod("produce", Object[].class));
    registerFunc("func-cons", FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, ResultCapture.class));

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 100);
    createOp(runtime, "func-cons", 101);
    runtime.registerDependency(100, 0, 101, 1);
    runtime.registerDependency(100, 1, 101, 0);

    runtime.setArguments(100, new Tuple[]{new Tuple(0, new Object[]{"and",
            5})});
    ResultCapture funcCapture = new ResultCapture();
    runtime.setArguments(101, new Tuple[]{new Tuple(2, funcCapture)});

    runtime.execute();

    Assert.assertEquals(5, funcCapture._iResult);
    Assert.assertEquals("and", funcCapture._sResult);
  }

  @Test(timeout = 20000)
  public void testNULLPropagationSingle() throws Throwable {
    clearCache();
    loadCoreOps();
    registerFunc("func-prod", FunctionalGenericProducer.class.getDeclaredMethod("produce", Object[].class));
    registerFunc("func-cons", StringConsumer.class.getDeclaredMethod("consume", String.class, ResultCapture.class));

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 100);
    createOp(runtime, "func-cons", 101);
    runtime.registerDependency(100, 0, 101, 0);

    runtime.setArguments(100, new Tuple[]{new Tuple(0, new Object[]{null})});
    ResultCapture funcCapture = new ResultCapture();
    runtime.setArguments(101, new Tuple[]{new Tuple(1, funcCapture)});

    runtime.execute();

    Assert.assertEquals(null, funcCapture._sResult);
  }

  @Test(timeout = 20000)
  public void testNULLPropagationCombine() throws Throwable {
    clearCache();
    loadCoreOps();
    registerFunc("func-prod", FunctionalGenericProducer.class.getDeclaredMethod("produce", Object[].class));
    registerFunc("func-cons", FunctionalConsumer.class.getDeclaredMethod("consume", int.class, String.class, ResultCapture.class));

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-prod", 100);
    createOp(runtime, "func-cons", 101);
    runtime.registerDependency(100, 0, 101, 1);
    runtime.registerDependency(100, 1, 101, 0);

    runtime.setArguments(100, new Tuple[]{new Tuple(0, new Object[]{null,
            5})});
    ResultCapture funcCapture = new ResultCapture();
    runtime.setArguments(101, new Tuple[]{new Tuple(2, funcCapture)});

    runtime.execute();

    Assert.assertEquals(5, funcCapture._iResult);
    Assert.assertEquals(null, funcCapture._sResult);
  }

  /**
   * Make sure compoundness also works when there was only one mapping to the compound input.
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  public void testCompoundSingleInput() throws Throwable {
    clearCache();
    loadCoreOps();
    registerFunc("func-gen-prod", FunctionalGenericProducer.class.getDeclaredMethod("produce", Object[].class));
    registerFunc("func-cons", SingleCompoundConsumer.class.getDeclaredMethod("consume", ResultCapture.class, int.class, Object[].class));

    OhuaRuntime runtime = new OhuaRuntime();
    createOp(runtime, "func-gen-prod", 100);
    createOp(runtime, "func-gen-prod", 101);
    createOp(runtime, "func-cons", 102);
    runtime.registerDependency(100, 0, 102, 1);
    runtime.registerDependency(101, 0, 102, 2);

    runtime.setArguments(100, new Tuple[]{new Tuple(0, new Object[]{5})});
    runtime.setArguments(101, new Tuple[]{new Tuple(0, new Object[]{"some"})});

    ResultCapture capture = new ResultCapture();
    runtime.setArguments(102, new Tuple[]{new Tuple(0, capture)});

    runtime.execute();

    Assert.assertEquals(5, capture._iResult);
    Assert.assertEquals("some", capture._sResult);
  }

  // FIXME once we have more comile-time info, this should probably throw an exception as only
  // one single compound argument is allowed per function.
//  @Test(timeout = 20000)
//  public void testInvalidCompoundness() throws Throwable {
//      clearCache();
//      loadCoreOps();
//      registerFunc("func-gen-prod", FunctionalGenericProducer.class.getDeclaredMethod("produce", Object[].class));
//      registerFunc("func-cons", CompoundConsumer.class.getDeclaredMethod("consume", Object[].class, Object[].class, MergeResultCapture.class));
//
//    FlowGraphCompiler compiler = new FlowGraphCompiler();
//      createOp(compiler, "func-gen-prod", 100);
//      createOp(compiler, "func-gen-prod", 101);
//      createOp(compiler, "func-cons", 102);
//    compiler.registerDependency(100, 0, 102, 0);
//    compiler.registerDependency(101, 0, 102, 1);
//
//    compiler.setArguments(100, new Tuple[] { new Tuple(0, new Object[] { 5 }) });
//    compiler.setArguments(101, new Tuple[] { new Tuple(0, new Object[] { "some" }) });
//
//    ResultCapture capture = new ResultCapture();
//    compiler.setArguments(102, new Tuple[] { new Tuple(1, capture) });
//
//    try {
//      compiler.compile(true);
//      Assert.fail();
//    }catch(MultiCompilationException mce){
//      Assert.assertTrue(mce.getFailures().size() == 1);
//      Assert.assertTrue(mce.getFailures().get(0).getCauze() == CompilationException.CAUSE.ENV_ARG_ASSIGNED_TO_ALREADY_ASSIGEND_SLOT);
//    }
//  }

  public static class FunctionalProducer {
    private int _count = 1;

    @defsfn
    public Object[] produce() {
      if (_count == 1) {
        _count--;
        return new Object[]{5,
                "some"};
      } else {
        return null;
      }
    }
  }

  public static class FunctionalGenericProducer {
    private int _count = 1;

    @defsfn
    public Object[] produce(Object[] toProduce) {
      if (_count == 1) {
        _count--;
        Object[] toProduce1 = toProduce;
        return toProduce1;
      } else {
        return null;
      }
    }
  }

  public static class FunctionalMerge {
    @defsfn
    public Object[] merge(int iLeft, String sLeft, int iRight, String sRight) {
      return new Object[]{iLeft,
              sLeft,
              iRight,
              sRight};
    }
  }

  public static class FunctionalPeek {
    @defsfn
    public Object[] peek(int num, String s) {
      // System.out.println("Peek: " + num + " : " + s);
      return new Object[]{num,
              s};
    }
  }

  public static class IntConsumer {
    @defsfn
    public void consume(int num, ResultCapture capture) {
      capture._iResult = num;
    }
  }

  public static class StringConsumer {
    @defsfn
    public void consume(String s, ResultCapture capture) {
      capture._sResult = s;
    }
  }

  public static class FunctionalConsumer {
    @defsfn
    public void consume(int num, String s, ResultCapture capture) {
//            System.out.println("Args: " + num + " : " + s);
      capture._iResult = num;
      capture._sResult = s;
    }
  }

  public static class FunctionalMergeConsumer extends FunctionalConsumer {
    @defsfn
    public void consume(int num1, String s1, int num2, String s2, MergeResultCapture capture) {
      super.consume(num1, s1, capture);
      capture._jResult = num2;
      capture._tResult = s2;
    }
  }

  public static class CompoundConsumer extends FunctionalMergeConsumer {
    @defsfn
    public void consume(Object[] comp1, Object[] comp2, MergeResultCapture capture) {
      super.consume((int) comp1[0], (String) comp1[1], (int) comp2[0], (String) comp2[1], capture);
    }
  }

  public static class SingleCompoundConsumer extends FunctionalConsumer {
    @defsfn
    public void consume(ResultCapture capture, int comp1, Object... comp2) {
      super.consume(comp1, (String) comp2[0], capture);
    }
  }

  public static class ResultCapture {
    int _iResult = -1;
    String _sResult = null;
  }

  public static class MergeResultCapture extends ResultCapture {
    int _jResult = -1;
    String _tResult = null;
  }

}
