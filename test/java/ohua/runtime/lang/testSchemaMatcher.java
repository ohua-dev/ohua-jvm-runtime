/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;


import ohua.runtime.lang.operator.*;
import ohua.runtime.test.AbstractRegressionTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.points.PacketFactory;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.lang.operator.FunctionalSchemaMatching.SchemaMatcher;
import ohua.lang.OneToNSupport;
import ohua.util.Tuple;
import ohua.lang.defsfn;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.daapi.DataAccessLayer;
import ohua.runtime.engine.daapi.DataPacket;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.points.VisitorFactory;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.lang.operator.LanguageDataFormat.LanguageDataPacket;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacketImpl;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.handers.DataPacketHandler;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.handers.EndOfStreamPacketHandler;

import ohua.runtime.exceptions.CompilationException;

public class testSchemaMatcher extends AbstractRegressionTestCase {
  private AbstractOperatorRuntime createOp(Class<? extends UserOperator> opClz) throws Throwable {
    FlowGraph graph = new FlowGraph();
    OperatorFactory.getInstance().setApplyDescriptorsForUserOperators(false);
    OperatorFactory.getInstance().setOperatorImplementationClass("test-op", opClz);
    OperatorFactory.getInstance().createUserOperator(graph, "test-op", "test-op-name");
    OperatorCore core = graph.getOperator("test-op-name");
    AbstractOperatorRuntime runtime = new NotificationBasedOperatorRuntime(core, new RuntimeProcessConfiguration());
    DataAccessLayer dal = new DataAccessLayer(runtime, new LanguageDataFormat());
    core.setDataLayer(dal);
    return runtime;
  }

  protected static AbstractOperatorRuntime createFunctionalOp(String fnName) throws Throwable {
    JavaSFNLinker.loadCoreOperators();
    SFNLinker.getInstance().setApplyDescriptorsForUserOperators(false);
    OperatorCore core =
            SFNLinker.getInstance().createUserOperatorCore(new FlowGraph(), fnName);
    AbstractOperatorRuntime runtime = new NotificationBasedOperatorRuntime(core, new RuntimeProcessConfiguration());
    core.setDataLayer(new DataAccessLayer(runtime, new LanguageDataFormat()));
    return runtime;
  }

  protected static void createInput(AbstractOperatorRuntime runtime, String portName) {
    InputPort in1 = new InputPort(runtime.getOp());
    in1.setPortName(portName);
    in1.registerPacketVisitor(VisitorFactory.createDataPacketVisitor(in1));
    in1.registerForEvent(InputPortEvents.DATA_PACKET_ARRIVAL, new DataPacketHandler());
    in1.registerPacketVisitor(VisitorFactory.createEndStreamPacketVisitor(in1));
    in1.registerForEvent(InputPortEvents.END_OF_STREAM_PACKET_ARRIVAL,
                         new EndOfStreamPacketHandler(new OhuaOperator(runtime)));
    in1.initComplete();
    runtime.getOp().addInputPort(in1);
    Arc arc1 = new Arc();
    AsynchronousArcImpl asyncImpl = new AsynchronousArcImpl(arc1);
    asyncImpl.disableDownstreamActivation();
    asyncImpl.disableUpstreamActivation();
    arc1.setImpl(asyncImpl);
    arc1.setTargetPort(in1);
  }

  protected static void createOutput(AbstractOperatorRuntime runtime, String portName) {
    OperatorCore core = runtime.getOp();
    OutputPort out = new OutputPort(core);
    out.setPortName(portName);
    out.initComplete();
    core.addOutputPort(out);
    Arc arc = new Arc();
    AsynchronousArcImpl asyncImpl = new AsynchronousArcImpl(arc);
    asyncImpl.disableDownstreamActivation();
    asyncImpl.disableUpstreamActivation();
    arc.setImpl(asyncImpl);
    arc.setSourcePort(out);
  }

  @Test
  public void testShutdown() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");

    TestFunctionalOp funcOp = new TestFunctionalOp();
    funcOp._parameterTypes = new Class<?>[] {Object.class, Object.class};
    AbstractSchemaMatcher matcher = new SchemaMatcher(funcOp, (UserOperator) core.getOp().getOperatorAlgorithm());
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, -1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 1 }, -1);
    matcher.prepare();

    /*
     * We simulate the situation where the first port is already done but the second one has the
     * EOS still pending. After this call both ports should have seen the EOS.
     */
    LinkedList<IMetaDataPacket> l = new LinkedList<>();
    l.add(new EndOfStreamPacketImpl(-1, SystemPhaseType.COMPUTATION));
    core.getOp().getInputPort("input-1").setHasSeenLastPacket(true);
    Assert.assertTrue(core.getOp().getInputPort("input-1").hasSeenLastPacket());
    core.getOp().getInputPort("input-2").setHasSeenLastPacket(false);
    core.getOp().getInputPort("input-2").getIncomingArc().enqueueBatch(l);
    Assert.assertFalse(core.getOp().getInputPort("input-2").hasSeenLastPacket());
    boolean dataIsAvailable = SchemaMatcherAccess.isCallDataAvailable(matcher);
    Assert.assertFalse(dataIsAvailable);
    Assert.assertTrue(core.getOp().getInputPort("input-1").getIncomingArc().isQueueEmpty());
    Assert.assertTrue(core.getOp().getInputPort("input-2").getIncomingArc().isQueueEmpty());
    Assert.assertTrue(core.getOp().getInputPort("input-2").hasSeenLastPacket());
  }

  /**
   * Matches a call like (func-b (func-a ) some-arg) where func-a returns two strings.
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  public void testImplicitSchemaMatch() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 2;
    fo._parameterTypes = new Class<?>[] { String.class,
//                                          String.class,
                                          List.class };
    fo._arguments = new Tuple[] { new Tuple(1, Collections.singletonList(8)) };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, -1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.prepare();

    // enqueue some data
    LinkedList<DataPacket> l = new LinkedList<>();
    LanguageDataPacket dp = new LanguageDataPacket();
    dp.setData(
//            new Object[] {
                    "string-a"
//                    , "string-b" }
    );
    l.add(dp);
    core.getOp().getInputPort("input-1").getIncomingArc().enqueueBatch(l);

    Object[] matched = matcher.matchInputSchema();
    Assert.assertEquals(2, matched.length);
    Assert.assertEquals("string-a", matched[0]);
    Assert.assertArrayEquals(Collections.singletonList(8).toArray(), ((List<?>) matched[1]).toArray());
  }

  /**
   * Matches code like (let [[arg1 arg2] (some-f )] (func-b arg1 (func-a ) arg2)) where func-a
   * returns two strings.
   *
   * @throws Throwable
   */
  @Test(timeout = 20000)
  public void testMixedSchemaMatch() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 4;
    fo._parameterTypes = new Class<?>[] { int.class,
                                          String.class,
//                                          String.class,
                                          int.class };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0,
                                                                    2 },
                                             1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 1 }, -1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-1", new Object[] { 5,
                                                10 });
    enqueueData(core, "input-2",
//            new Object[] {
                    "string-a"
//                    , "string-b" }
    );

    Object[] matched = matcher.matchInputSchema();
//    System.out.println("matched: " + Arrays.deepToString(matched));
    Assert.assertEquals(3, matched.length);
    Assert.assertArrayEquals(new Object[] { 5,
                                            "string-a",
//                                            "string-b",
                                            10 },
                             matched);
  }

  @Test(timeout = 20000)
  public void testMultipleImplicit() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 5;
    fo._parameterTypes = new Class<?>[] { String.class,
//                                          String.class,
//                                          int.class,
//                                          int.class,
                                          int.class };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, -1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 1 }, -1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-1",
//            new Object[] {
                    "string-a"
//                    , "string-b" }
    );
    enqueueData(core, "input-2",
//            new Object[] { 5,
                                                10
//                    , 20 }
    );

    Object[] matched = matcher.matchInputSchema();
//    System.out.println("matched: " + Arrays.deepToString(matched));
    Assert.assertEquals(2, matched.length);
    Assert.assertArrayEquals(new Object[] { "string-a",
//                                            "string-b",
//                                            5,
                                            10,
//                                            20
    },
                             matched);
  }

  @Test(timeout = 20000)
  public void testMultipleImplicitNested() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");
    createInput(core, "input-3");
    createInput(core, "input-4");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 5;
    fo._parameterTypes = new Class<?>[] { float.class,
                                          String.class,
//                                          String.class,
                                          int.class,
//                                          int.class,
                                          float.class };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, 1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 1 }, -1);
    matcher.registerInputExplicitSchemaMatch("input-3", new int[] { 2 }, -1);
    matcher.registerInputExplicitSchemaMatch("input-4", new int[] { 3 }, 1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    inputs.add("input-3");
    inputs.add("input-4");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-1", new Object[] { 5.0f });
    enqueueData(core, "input-2",
//            new Object[] {
            "string-a"
//            , "string-b"}
    );
    enqueueData(core, "input-3",
//            new Object[] {
                    5
//                    , 10 }
    );
    enqueueData(core, "input-4", new Object[] { 10.0f });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println("matched: " + Arrays.deepToString(matched));
    Assert.assertEquals(4, matched.length);
    Assert.assertArrayEquals(new Object[] { 5.0f,
                                            "string-a",
//                                            "string-b",
                                            5,
//                                            10,
                                            10.0f },
                             matched);
  }

  @Test(timeout = 20000)
  public void testImplicitWithArgs() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 2;
    fo._parameterTypes = new Class<?>[] { String.class,
//                                          String.class,
                                          float.class };
    fo._arguments = new Tuple[] { new Tuple(1, 10.0f) };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    // there is does something else now! -> takes the output just as is, but does not splice it into the args array.
//    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, -1);
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, -1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-1",
//            new Object[] {
            "string-a"
//            , "string-b"
//    }
  );

    Object[] matched = matcher.matchInputSchema();
//    System.out.println("matched: " + Arrays.deepToString(matched));
    Assert.assertArrayEquals(new Object[] { "string-a",
//                                            "string-b",
                                            10.0f },
                             matched);
  }

  @Test(timeout = 20000)
  public void testExplicitWithArgs() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 2;
    fo._parameterTypes = new Class<?>[] { String.class,
                                          String.class,
                                          float.class };
    fo._arguments = new Tuple[] { new Tuple(2, 10.0f) };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0,
                                                                    1 },
                                             1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-1", new Object[] { "string-a",
                                                "string-b" });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println("matched: " + Arrays.deepToString(matched));
    Assert.assertArrayEquals(new Object[] { "string-a",
                                            "string-b",
                                            10.0f },
                             matched);
  }

  @Test
  public void testMixedExplicitImplicitExplicitMatch() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");
    createInput(core, "input-3");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 4;
    fo._parameterTypes = new Class<?>[] { String.class,
                                          String.class,
                                          String.class,
                                          String.class,
//                                          String.class,
                                          int.class };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-3", new int[] { 0,
                                                                    1,
                                                                    2 },
                                             1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 3 }, -1); // implicit
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 4 }, 1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    inputs.add("input-3");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-3", new Object[] { "string-1",
                                                "string-2",
                                                "string-3" });
    enqueueData(core, "input-2",
//            new Object[] {
                    "implicit-1"
//                    , "implicit-2" }
    );
    enqueueData(core, "input-1", new Object[] { 10 });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(new Object[] { "string-1",
                                            "string-2",
                                            "string-3",
                                            "implicit-1",
//                                            "implicit-2",
                                            10 },
                             matched);
  }

  @Ignore // We don't have a `merge` right now
  @Test
  public void testCompoundArgs() throws Throwable {
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 1;
    fo._isVarArgs = true;
    fo._parameterTypes = new Class<?>[] { Object[].class };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, 1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.prepare();

    // prepares the compound stuff
    enqueueData(core, "input-1", new Object[]{ 10 });
    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(new Object[] { new Object[] {10}}, matched);

    // uses the compound stuff
    enqueueData(core, "input-1", new Object[]{ 20 });
    matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(new Object[] { new Object[] {20}}, matched);
  }

  /**
   * Makes sure that the compoundness algorithm adheres to the order the arguments are
   * specified.
   */
  @Test
  public void testCompoundnessOrder() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 1;
    fo._parameterTypes = new Class<?>[] { Object[].class };
    fo._isVarArgs = true;

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 1 }, 1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 0 }, 1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-1", new Object[] { "string-a" });
    enqueueData(core, "input-2", new Object[] { 10 });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertEquals(1, matched.length);
    Assert.assertEquals(Object[].class, matched[0].getClass());
    Assert.assertEquals(10, ((Object[]) matched[0])[0]);
    Assert.assertEquals("string-a", ((Object[]) matched[0])[1]);
  }

  private void enqueueData(AbstractOperatorRuntime runtime, String port, Object data) {
    LinkedList<DataPacket> l = new LinkedList<>();
    LanguageDataPacket dp = new LanguageDataPacket();
    dp.setData(data);
    l.add(dp);
    runtime.getOp().getInputPort(port).getIncomingArc().enqueueBatch(l);
  }

  @Test
  public void testCompoundnessOrder2() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 1;
    fo._parameterTypes = new Class<?>[] { Object[].class };
    fo._isVarArgs = true;

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 1,
                                                                    2,
                                                                    3,
                                                                    4 },
                                             1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 0 }, 1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-1", new Object[] { "string-1",
                                                "string-2",
                                                "string-3",
                                                "string-4" });
    enqueueData(core, "input-2", new Object[] { 10 });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertEquals(1, matched.length);
    Assert.assertEquals(Object[].class, matched[0].getClass());
    Assert.assertEquals(10, ((Object[]) matched[0])[0]);
    Assert.assertEquals("string-1", ((Object[]) matched[0])[1]);
    Assert.assertEquals("string-2", ((Object[]) matched[0])[2]);
    Assert.assertEquals("string-3", ((Object[]) matched[0])[3]);
    Assert.assertEquals("string-4", ((Object[]) matched[0])[4]);
  }

  @Test
  public void testEnvironmentVarsOnly() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 0;
    fo._parameterTypes = new Class<?>[] { int.class };
    fo._arguments = new Tuple[] { new Tuple(0, 5) };

    AbstractSchemaMatcher matcher = new FunctionalSchemaMatching.FunctionalSourceSchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    List<String> inputs = new ArrayList<>();
    matcher.prepare();

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(new Object[] { 5 }, matched);
  }

  @Test
  public void testCompoundnessOrderImplicitMatch() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");
    createInput(core, "input-3");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 1;
    fo._parameterTypes = new Class<?>[] { Object[].class };
    fo._isVarArgs = true;

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-3", new int[] { 0,
                                                                    1,
                                                                    2 },
                                             1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 3 }, -1); // implicit
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 4 }, 1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    inputs.add("input-3");
    matcher.prepare();

    // enqueue some data
    enqueueData(core, "input-3", new Object[] { "string-1",
                                                "string-2",
                                                "string-3" });
    enqueueData(core, "input-2",
//            new Object[] { "implicit-1",
                    "implicit-2"
//            }
    );
    enqueueData(core, "input-1", new Object[] { 10 });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(
            new Object[]{ // call arguments array
              new Object[] { // var arg array
                      "string-1",
                       "string-2",
                       "string-3",
//                       "implicit-1",
                       "implicit-2",
                       10 } },
             matched);
  }

  @Test
  public void testCompoundnessWithSingleImplicitFlowInput() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 1;
    fo._parameterTypes = new Class<?>[] { Object[].class };
    fo._isVarArgs = true;

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, -1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.prepare();

    enqueueData(core, "input-1", 10 );

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(
            new Object[] { // call (arguments) array
              new Object[] { 10 } // var arg array
    }, matched);
  }

  @Test
  public void testCompoundnessWithSingleImplicitFlowInputAndMany() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 1;
    fo._parameterTypes = new Class<?>[] { Object[].class };
    fo._isVarArgs = true;

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, -1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.prepare();

    enqueueData(core, "input-1", new Object[] { 10,
                                                20 });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(
            new Object[] { // call (arguments) array
//              new Object[] { // compound array -> mimic Java behavior when an array is the only element of a var args array position: it becomes the var args array.
                new Object[] { 10, 20 } // implicit match array
//            }
  },
                             matched);
  }

  @Test
  public void testCompoundnessWithSingleExplicitFlowInput() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 1;
    fo._parameterTypes = new Class<?>[] { Object[].class };
    fo._isVarArgs = true;

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0,
                                                                    1 },
                                             1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.prepare();

    enqueueData(core, "input-1", new Object[] { 10,
                                                20 });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(new Object[] { new Object[] { 10,
                                                           20 } },
                             matched);
  }

  @Test
  public void testCompoundOnlyWithImplicitInput() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createInput(core, "input-2");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 2;
    fo._parameterTypes = new Class<?>[] { Object[].class };
    fo._isVarArgs = true;

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 1 }, -1);
    matcher.registerInputExplicitSchemaMatch("input-2", new int[] { 0 }, -1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    inputs.add("input-2");
    matcher.prepare();

    enqueueData(core, "input-1",
//            new Object[] {
                    10
//                    , 20, 30 }
    );
    enqueueData(core, "input-2",
//            new Object[] {
                    40
//                    , 50 }
    );

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(
            new Object[]{ // call arguments array
             new Object[] { // var args array
                     40,
        //            50,
                    10,
        //            20,
        //            30
    }}, matched);
  }

  @Test
  public void testCompoundOnlyTyped() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._flowArgCount = 1;
    fo._parameterTypes = new Class<?>[] { Integer[].class };
    fo._isVarArgs = true;

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 0 }, -1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.prepare();

    enqueueData(core, "input-1",
//            new int[] {
              10
//                    , 20, 30 }
    );

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(
            new Object[] { // call arguments array
              new Integer[] { // var args array
                      10
              } },
            matched);
  }

  @Test
  public void testSwitchWithTwoFalseOutputs() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createOutput(core, "output-1");
    createOutput(core, "output-2");
    createOutput(core, "output-3");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._parameterTypes = new Class<?>[] { int.class,
                                          int.class,
                                          int.class };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());

    // register to satisfy prepare
    matcher.registerInputExplicitSchemaMatch("input-1", new int[]{0,1,2}, 1);

    matcher.registerOutputExplicitSchemaMatch("output-1", new int[] { 0 });
    matcher.registerOutputExplicitSchemaMatch("output-2", new int[] { 1 });
    matcher.registerOutputExplicitSchemaMatch("output-3", new int[] { 1 });
    List<String> outputs = new ArrayList<>();
    outputs.add("output-1");
    outputs.add("output-2");
    outputs.add("output-3");
    matcher.prepare();

    matcher.matchOutputSchema(new Object[] { true,
                                             false });

    LanguageDataPacket mp = (LanguageDataPacket) core.getOp().getOutputPort("output-1").getOutgoingArcs().get(0).getData().get();
    Assert.assertEquals(true, mp.getData());
    mp = (LanguageDataPacket) core.getOp().getOutputPort("output-2").getOutgoingArcs().get(0).getData().get();
    Assert.assertEquals(false, mp.getData());
    mp = (LanguageDataPacket) core.getOp().getOutputPort("output-3").getOutgoingArcs().get(0).getData().get();
    Assert.assertEquals(false, mp.getData());
  }

  @Test
  public void testDROPControl() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createOutput(core, "output-1");
    createOutput(core, "output-2");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._parameterTypes = new Class<?>[] { int.class,
            int.class};

    AbstractSchemaMatcher matcher = new DataflowSchemaMatching.DataflowSchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    // register to satisfy prepare
    matcher.registerInputExplicitSchemaMatch("input-1", new int[]{0,1}, 1);

    matcher.registerOutputExplicitSchemaMatch("output-1", new int[] { 0 });
    matcher.registerOutputExplicitSchemaMatch("output-2", new int[] { 1 });
    List<String> outputs = new ArrayList<>();
    outputs.add("output-1");
    outputs.add("output-2");
    matcher.prepare();

    matcher.matchOutputSchema(new Object[] { true, DataflowFunction.Control.DROP });

    LanguageDataPacket mp = (LanguageDataPacket) core.getOp().getOutputPort("output-1").getOutgoingArcs().get(0).getData().get();
    Assert.assertEquals(true, mp.getData());
    Maybe<Object> maybe = core.getOp().getOutputPort("output-2").getOutgoingArcs().get(0).getData();
    Assert.assertFalse(maybe.isPresent());
  }

  @Test
  public void testDROPControlTwoOutToSamePort() throws Throwable {
    // prepare a fake graph
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");
    createOutput(core, "output-1");


    TestFunctionalOp fo = new TestFunctionalOp();
    fo._parameterTypes = new Class<?>[] { int.class,
            int.class};

    AbstractSchemaMatcher matcher = new DataflowSchemaMatching.DataflowSchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    // register to satisfy prepare
    matcher.registerInputExplicitSchemaMatch("input-1", new int[]{0,1}, 1);

    matcher.registerOutputExplicitSchemaMatch("output-1", new int[] { 0 , 1});
    List<String> outputs = new ArrayList<>();
    outputs.add("output-1");
    matcher.prepare();

    matcher.matchOutputSchema(new Object[] { 10, DataflowFunction.Control.DROP });
    Maybe<Object> maybe = core.getOp().getOutputPort("output-1").getOutgoingArcs().get(0).getData();
    Assert.assertFalse(maybe.isPresent());

    matcher.matchOutputSchema(new Object[] { DataflowFunction.Control.DROP, 20 });
    LanguageDataPacket mp = (LanguageDataPacket) core.getOp().getOutputPort("output-1").getOutgoingArcs().get(0).getData().get();
    Assert.assertArrayEquals(new Object[]{ 10, 20 }, (Object[])mp.getData());
  }

  /**
   * This tests whether compilation understands the correct number of flow arguments in case of
   * compound arguments. The test succeeds when it does not throw an arity exception.
   *
   * @throws Throwable
   */
  @Test
  public void testCompoundnessValidation() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/apply");
    createInput(core, "input-1");
    core.getOp().setOperatorName("apply-101");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 1,
                                                                                               2},
                                                                                   1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setArguments(new Tuple[] { new Tuple(0, new Object[] { new int[0] })});
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).compile(true);
  }

  /**
   * If we have a compound argument and map global variables to the function then the runtime
   * has to understand that it needs to merge the arguments into the array of the last argument.
   *
   * @throws Throwable
   */
  @Test
  public void testCompoundnessMixedWithGlobal() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/apply");
    createInput(core, "input-1");
    createOutput(core, "output-1");
    core.getOp().setOperatorName("apply-101");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 0,
                                                                                               1 },
                                                                                   1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setArguments( new Tuple[] {new Tuple(2, 10 )});
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { 0 });
    core.getOp().getOperatorAlgorithm().prepare();

    // runtime
    enqueueData(core, "input-1", new Object[] { StatefulFunction.resolve(new ClojureTestOps.AddOperator()),
                                                5 });
    core.getOp().getOperatorAlgorithm().runProcessRoutine();

    LanguageDataPacket mp = (LanguageDataPacket) core.getOp().getOutputPort("output-1").getOutgoingArcs().get(0).getData().get();
    Assert.assertEquals(15L, mp.getData());
  }

  @Test
  public void testCompoundnessSingleCompoundArg() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/oneToN");
    createInput(core, "input-1");
    createOutput(core, "output-1");
    core.getOp().setOperatorName("one-to-n-101");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 0,
                                                                                               1 },
                                                                                   1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { -1 });
    core.getOp().getOperatorAlgorithm().prepare();

    // runtime
    enqueueData(core, "input-1", new Object[] { 5,
                                                5 });
    core.getOp().getOperatorAlgorithm().runProcessRoutine();

    LanguageDataPacket mp = (LanguageDataPacket) core.getOp().getOutputPort("output-1").getOutgoingArcs().get(0).getData().get();
    Assert.assertTrue(mp.getData() instanceof OneToNSupport.OneToNData);
    Assert.assertEquals(5, (int)((OneToNSupport.OneToNData)mp.getData())._s);
    Assert.assertEquals(5, ((OneToNSupport.OneToNData)mp.getData())._t);
  }

  @Test
  public void testSingleInputPassedToMoreThanOneSlot() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/size");
    createInput(core, "input-1");
    createOutput(core, "output-1");
    core.getOp().setOperatorName("size-101");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 0 }, 1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { -1 });
    core.getOp().getOperatorAlgorithm().prepare();

    // runtime
    enqueueData(core, "input-1", new Object[] { Collections.singleton(5) });
    core.getOp().getOperatorAlgorithm().runProcessRoutine();

    LanguageDataPacket mp = (LanguageDataPacket) core.getOp().getOutputPort("output-1").getOutgoingArcs().get(0).getData().get();
    Assert.assertEquals(1, mp.getData());
  }

  @Test
  public void testMixedArgs() throws Throwable {
    AbstractOperatorRuntime core = createOp(TestOp.class);
    createInput(core, "input-1");

    TestFunctionalOp fo = new TestFunctionalOp();
    fo._arguments = new Tuple[] { new Tuple(0, 10) };
    fo._flowArgCount = 1;
    fo._parameterTypes = new Class<?>[] { int.class,
                                          int.class };

    AbstractSchemaMatcher matcher = new SchemaMatcher(fo, (UserOperator) core.getOp().getOperatorAlgorithm());
    matcher.registerInputExplicitSchemaMatch("input-1", new int[] { 1 }, 1);
    List<String> inputs = new ArrayList<>();
    inputs.add("input-1");
    matcher.compile(inputs, Collections.emptyList());

    matcher.prepare();
    enqueueData(core, "input-1", new Object[] { 20 });

    Object[] matched = matcher.matchInputSchema();
//    System.out.println(Arrays.deepToString(matched));
    Assert.assertArrayEquals(new Object[] { 10, 20}, matched);
  }

  @Test
  public void testSingleInputBug() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/oneToN");
    createInput(core, "input-1");
    createOutput(core, "output-1");
    core.getOp().setOperatorName("one-to-n-101");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 0 },
            1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { -1 });
    try {
      ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).compile(true);
      Assert.fail();
    }catch(CompilationException ce){
      Assert.assertEquals(CompilationException.CAUSE.ARITY_TOO_FEW, ce.getCauze());
    }
  }

  @Test
  public void testEmptyVarArgsArray() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/ifThenElse");
    core.getOp().setOperatorName("ifThenElse-101");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setArguments( new Tuple[] {new Tuple(0, null )});
    createOutput(core, "output-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { 0 });
    createOutput(core, "output-2");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { 1 });
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).compile(true);
  }

  @Test
  public void testSelectNotReady1() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/select");
    core.getOp().setOperatorName("select-1");
    createInput(core, "input-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 1 },
            1);
    createInput(core, "input-2");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 0 },
            1);
    createInput(core, "input-3");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 2 },
            1);
    createOutput(core, "output-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { -1 });

    core.getOp().getOperatorAlgorithm().prepare();

    // "normal" case
    LinkedList<IMetaDataPacket> l = new LinkedList<>();
    l.add(PacketFactory.createEndSignalPacket(core.getOp().getLevel(), SystemPhaseType.COMPUTATION));
    core.getOp().getInputPort("input-1").getIncomingArc().enqueueBatch(l);

    // here you should not load anything
    core.getOp().getOperatorAlgorithm().runProcessRoutine();

    // this test passes when no exception occurs
  }

  @Test
  public void testSelectNotReady2() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/select");
    core.getOp().setOperatorName("select-1");
    createInput(core, "input-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 1 },
            1);
    createInput(core, "input-2");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 0 },
            1);
    createInput(core, "input-3");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 2 },
            1);
    createOutput(core, "output-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { -1 });

    core.getOp().getOperatorAlgorithm().prepare();

    // this may happen when the select is actually never being executed (because it lives on a branch itself that is never executed)
    // and the EOS among the other (non-strict) ports is still pending.
    LinkedList<IMetaDataPacket> l = new LinkedList<>();
    l.add(PacketFactory.createEndSignalPacket(core.getOp().getLevel(), SystemPhaseType.COMPUTATION));
    core.getOp().getInputPort("input-2").getIncomingArc().enqueueBatch(l);

    // here you should not load anything
    core.getOp().getOperatorAlgorithm().runProcessRoutine();

    // this test passes when no exception occurs
  }

  @Test
  public void testSelectNotReady3() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/select");
    core.getOp().setOperatorName("select-1");
    createInput(core, "input-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 1 },
            1);
    createInput(core, "input-2");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 0 },
            1);
    createInput(core, "input-3");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 2 },
            1);
    createOutput(core, "output-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(new int[] { -1 });

    core.getOp().getOperatorAlgorithm().prepare();

    // this may happen when the select is actually never being executed (because it lives on a branch itself that is never executed)
    // and the EOS among the other (non-strict) ports is still pending.
    LinkedList<IMetaDataPacket> l = new LinkedList<>();
    l.add(PacketFactory.createEndSignalPacket(core.getOp().getLevel(), SystemPhaseType.COMPUTATION));
    core.getOp().getInputPort("input-3").getIncomingArc().enqueueBatch(l);

    // here you should not load anything
    core.getOp().getOperatorAlgorithm().runProcessRoutine();

    // this test passes when no exception occurs
  }

  @Test
  public void testContinuationsEnvArgsFirst() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/algoIn");
    core.getOp().setOperatorName("algo-in-1");
    createInput(core, "input-1");
    createOutput(core, "output-1");
    createOutput(core, "output-2");
    // first the environment arg and then the dataflow arg
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setArguments(new Tuple[] { new Tuple(0, null), new Tuple(1, 10)});
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 2 }, 1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(0, new int[] {0});
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(1, new int[] {1});

    core.getOp().getOperatorAlgorithm().prepare();
    core.getOp().getInputPorts().stream().forEach(i -> i.setHasSeenLastPacket(false));

    // here you should not load anything
    core.getOp().getOperatorAlgorithm().runProcessRoutine();

    // nothing arrived, so the call should not be executed, i.e., nothing should have been sent.
    Assert.assertTrue(core.getOp().getOutputPorts().stream().map(OutputPort::getOutgoingArcs).flatMap(List::stream).allMatch(Arc::isQueueEmpty));
  }

  @Test
  public void testContinuationsEnvArgsLast() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/algoIn");
    core.getOp().setOperatorName("algo-in-1");
    createInput(core, "input-1");
    createOutput(core, "output-1");
    createOutput(core, "output-2");
    // first the environment arg and then the dataflow arg
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setArguments(new Tuple[] { new Tuple(0, null), new Tuple(2, 10)});
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] { 1 }, 1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(0, new int[] {0});
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(1, new int[] {1});

    core.getOp().getOperatorAlgorithm().prepare();
    core.getOp().getInputPorts().stream().forEach(i -> i.setHasSeenLastPacket(false));

    // here you should not load anything
    core.getOp().getOperatorAlgorithm().runProcessRoutine();

    // nothing arrived, so the call should not be executed, i.e., nothing should have been sent.
    Assert.assertTrue(core.getOp().getOutputPorts().stream().map(OutputPort::getOutgoingArcs).flatMap(List::stream).allMatch(Arc::isQueueEmpty));
  }

  @Test
  public void testContinuationsEnvArgOnly() throws Throwable {
    AbstractOperatorRuntime core = createFunctionalOp("ohua.lang/algoIn");
    core.getOp().setOperatorName("algo-in-1");
    createOutput(core, "output-1");
    // first the environment arg and then the dataflow arg
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setArguments(new Tuple[] { new Tuple(0, null), new Tuple(1, 10)});
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(0, new int[] {0});

    core.getOp().getOperatorAlgorithm().prepare();

    // we should run once and emit a single value
    core.getOp().getOperatorAlgorithm().runProcessRoutine();
    Assert.assertTrue(core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().allMatch(a -> a.getLoadEstimate() == 1));
    core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().forEach(Arc::sweep);

    core.getOp().getOperatorAlgorithm().runProcessRoutine();
//    System.out.println(core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().map(Arc::getLoadEstimate).collect(Collectors.toList()));
//    System.out.println(core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().map(Arc::peek).map(Maybe::get).collect(Collectors.toList()));
    Assert.assertTrue(core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().allMatch(Arc::isQueueEmpty));
  }

  public static class TestOp extends UserOperator {
    @Override
    public Object getState() {
      return null;
    }

    @Override
    public void setState(Object state) {
    }

    @Override
    public void prepare() {
    }

    @Override
    public void runProcessRoutine() {
    }

    @Override
    public void cleanup() {
    }
  }

  public static class TestSF {
    @defsfn
    public Object[] testImplicitDest(String a, String b, List<?> c) {
      return null;
    }
  }

//  private void vargsFunc(String... data){
//    System.out.println(Arrays.deepToString(data));
//  }
//
//  @Test
//  public void testJavaVarArgs() throws Throwable {
//    vargsFunc(new String[]{"hello"});
//  }

  private class TestFunctionalOp implements IFunctionalOperator {

    int _flowArgCount = 0;
    Class<?>[] _parameterTypes = new Class<?>[0];
    Tuple<Integer, Object>[] _arguments = new Tuple[0];
    boolean _isVarArgs = false;
    Class<?> _returnType = Object[].class;

    public boolean isAlgoVarArgs() {
      return _isVarArgs;
    }

    @Override
    public int getFlowArgumentCount() {
      return _flowArgCount;
    }

    public int getFlowFormalsCount() {
      return getFlowArgumentCount();
    }

    @Override
    public Class<?> getParameterType(int formalSchemaIndex) {
      return _parameterTypes[formalSchemaIndex];
    }

    @Override
    public Class<?>[] getFormalArguments() {
      return _parameterTypes;
    }

    @Override
    public Tuple<Integer, Object>[] getEnvironmentArguments() {
      return _arguments;
    }

    @Override
    public Annotation[] getParameterAnnotation(int formalSchemaIndex) {
      return null;
    }

    @Override
    public String getParameterName(int formalSchemaIndex) {
      return null;
    }

    @Override
    public String getFunctionName() {
      return null;
    }

    @Override
    public void compile(boolean typeSensitive) throws CompilationException {
    }

    @Override
    public void runSafetyAnalysis(boolean strict) throws CompilationException {
    }

    @Override
    public void setExplicitInputSchemaMatch(int[] explicitTargetMatching, int matchType) {
    }

    @Override
    public void setExplicitInputSchemaMatch(int portIdx, int[] explicitTargetMatching, int matchType) {
    }

    @Override
    public void setExplicitOutputSchemaMatch(int[] explicitSourceMatching) {
    }

    @Override
    public void setExplicitOutputSchemaMatch(int portIdx, int[] explicitSourceMatching) {
    }

    @Override
    public void setArguments(Tuple<Integer, Object>[] arguments) {
    }

    @Override
    public Class<?> getReturnType() {
      return _returnType;
    }
  }

  public class DeadEnd extends AsynchronousArcImpl {

    public DeadEnd(Arc arc) {
      super(arc);
    }

    protected void activateDownstream() {
      // don't do it!
    }

    protected void activateUpstream() {
      // don't do it!
    }
  }

}
