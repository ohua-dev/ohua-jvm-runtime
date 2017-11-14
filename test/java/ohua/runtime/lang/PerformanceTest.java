/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.NotificationBasedOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.lang.operator.AbstractFunctionalOperator;
import ohua.runtime.lang.operator.LanguageDataFormat;
import ohua.util.Tuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Run this test from Leiningen using this in any Clojure file:
 * (.(PerformanceTest. ) testContinuationsPerformance)
 *
 * Note, that this does not only test continuations but the whole operator framework.
 *
 * Created by sertel on 3/3/17.
 */
public class PerformanceTest {

  public void testContinuationsPerformance() throws Throwable {
    OperatorFactory operatorFactory = OperatorFactory.create();
    System.out.println("Starting performance test");
    AbstractOperatorRuntime core = testSchemaMatcher.createFunctionalOp(operatorFactory, "com.ohua.lang/algo-in");
    core.getOp().setOperatorName("algo-in-1");
    testSchemaMatcher.createInput(core, "input-1");
    testSchemaMatcher.createOutput(core, "output-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setArguments(new Tuple[] {
                    new Tuple(0, null)
            }
    );
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] {1}, 1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(0, new int[] {0});

    ((NotificationBasedOperatorRuntime)core).defineQuanta(Integer.MAX_VALUE);
    core.getOp().getOperatorAlgorithm().prepare();

    int testSize = 1000;
    ArrayList l = new ArrayList(testSize);
    LanguageDataFormat.LanguageDataPacket p = new LanguageDataFormat.LanguageDataPacket();
    p.setData(new Object[]{new Object[]{1000, 200}});
    for (int j = 0; j < testSize; j++) {
      l.add(p);
    }

    System.out.println("initialized");
    Thread.sleep(10000);
    System.out.println("starting ...");

    int testRuns = 200000;
    for(int i=0;i<testRuns;i++) {
      ((NotificationBasedOperatorRuntime)core).resetQuanta();
      core.getOp().getInputPort("input-1").getIncomingArc().enqueueBatch(l);

      core.getOp().getOperatorAlgorithm().runProcessRoutine();
      core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().forEach(Arc::sweep);
//    System.out.println(core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().map(Arc::getLoadEstimate).collect(Collectors.toList()));
//    System.out.println(core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().map(Arc::peek).map(Maybe::get).collect(Collectors.toList()));
    }
    System.out.println("performance test done.");
  }

  @Test
  public void testContinuationsPerformance2() throws Throwable {
    System.out.println("Starting performance test");
    OperatorFactory operatorFactory = OperatorFactory.create();
    AbstractOperatorRuntime core = testSchemaMatcher.createFunctionalOp(operatorFactory, "com.ohua.lang/algo-in");
    core.getOp().setOperatorName("algo-in-1");
    testSchemaMatcher.createInput(core, "input-1");
    testSchemaMatcher.createOutput(core, "output-1");
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setArguments(new Tuple[] {
                    new Tuple(0, null),
                    new Tuple(2, new HashSet()),
                    new Tuple(3, new HashSet()),
                    new Tuple(4, new HashSet()),
                    new Tuple(5, new HashSet()),
                    new Tuple(6, new HashSet()),
                    new Tuple(7, new HashSet()),
                    new Tuple(8, new HashSet()),
            }
    );
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitInputSchemaMatch(new int[] {1}, 1);
    ((AbstractFunctionalOperator) core.getOp().getOperatorAlgorithm()).setExplicitOutputSchemaMatch(0, new int[] {0});

    ((NotificationBasedOperatorRuntime)core).defineQuanta(Integer.MAX_VALUE);
    core.getOp().getOperatorAlgorithm().prepare();

    int testSize = 1000;
    ArrayList l = new ArrayList(testSize);
    LanguageDataFormat.LanguageDataPacket p = new LanguageDataFormat.LanguageDataPacket();
    p.setData(new Object[]{new Object[]{1000, 200}});
    for (int j = 0; j < testSize; j++) {
      l.add(p);
    }

    System.out.println("initialized");
    Thread.sleep(10000);
    System.out.println("starting ...");

    int testRuns = 200000;
//    int testRuns = 20;
    for(int i=0;i<testRuns;i++) {
      ((NotificationBasedOperatorRuntime)core).resetQuanta();
      core.getOp().getInputPort("input-1").getIncomingArc().enqueueBatch(l);
//      System.out.println("running");
      core.getOp().getOperatorAlgorithm().runProcessRoutine();
      core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().forEach(Arc::sweep);
//    System.out.println(core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().map(Arc::getLoadEstimate).collect(Collectors.toList()));
//    System.out.println(core.getOp().getOutputPort("output-1").getOutgoingArcs().stream().map(Arc::peek).map(Maybe::get).collect(Collectors.toList()));
    }
    System.out.println("performance test done.");
  }

}
