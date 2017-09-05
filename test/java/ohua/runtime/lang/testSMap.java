/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import clojure.lang.Compiler;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorLibrary;
import ohua.runtime.engine.utils.GraphVisualizer;
import ohua.runtime.test.AbstractFlowTestCase;
import ohua.util.Tuple;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Ignore // We don't have `merge` right now
public class testSMap extends AbstractFlowTestCase {

  @SuppressWarnings("unchecked")
  private OhuaRuntime prepareProgram() throws Throwable {
    GraphVisualizer.PRINT_FLOW_GRAPH = super.getTestMethodOutputDirectory() + "graph";
    Map<String, String> ops = new HashMap<String, String>();
    ops.put("add", ClojureTestOps.AddOperator.class.getName());
    ops.put("subtract", ClojureTestOps.SubtractOperator.class.getName());
    OperatorLibrary.registerOperators(ops, getTestMethodOutputDirectory() + "test-registry.xml");
    Linker.loadCoreOperators();
    Linker.loadAppOperators(getTestMethodOutputDirectory() + "test-registry.xml");

    String code =
            "(doto (new ohua.runtime.lang.OhuaRuntime)"
                    + "(.createOperator \"com.ohua.lang/capture\" 100)"
                    + "(.createOperator \"merge\" 101)"
                    + "(.createOperator \"com.ohua.lang/id\" 102)"
                    + "(.createOperator \"com.ohua.lang/size\" 103)"
                    + "(.createOperator \"merge\" 104)"
                    + "(.createOperator \"com.ohua.lang/smap-fun\" 105)"
                    + "(.createOperator \"com.ohua.lang/one-to-n\" 106)"
                    + "(.createOperator \"com.ohua.lang/collect\" 107)"
                    + "(.createOperator \"com.ohua.lang/one-to-n\" 108)"
                    + "(.createOperator \"merge\" 109)"
                    + "(.createOperator \"ifThenElse\" 110)"
                    + "(.createOperator \"add\" 112)"
                    + "(.createOperator \"subtract\" 113)"
                    + "(.registerDependency 101 -1 100 0)"
                    + "(.registerDependency 104 -1 101 1)"
                    + "(.registerDependency 106 -1 105 0)"
                    + "(.registerDependency 107 -1 104 1)"
                    + "(.registerDependency 108 -1 107 0)"
                    + "(.registerDependency 109 -1 107 1)"
                    + "(.registerDependency 112 -1 109 0)"
                    + "(.registerDependency 113 -1 109 1)"
                    + "(.registerDependency 102 -1 103 0)"
                    + "(.registerDependency 103 -1 106 0)"
                    + "(.registerDependency 102 -1 106 1)"
                    + "(.registerDependency 103 -1 108 0)"
                    + "(.registerDependency 103 -1 108 1)"
                    + "(.registerDependency 105 0 110 1)"
                    + "(.registerDependency 105 0 112 0)"
                    + "(.registerDependency 105 0 113 0)"
                    + "(.registerDependency 110 0 112 -1)"
                    + "(.registerDependency 110 1 113 -1)"
//        +"(.setArguments 100 (clojure.core/into-array java.lang.Object (clojure.core/seq [result])))"
//        +"(.setArguments 102 (clojure.core/into-array java.lang.Object (clojure.core/seq [data])))"
                    + "(.setArguments 110 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 0) (clojure.core/reify com.ohua.lang.Condition (check [this args] (< (clojure.core/nth args 0) 3))) ))))"
                    + "(.setArguments 112 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 1) 100))))"
                    + "(.setArguments 113 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 1) 3))))"
//        +"(.execute {\"shared-env-vars\" {}})";
                    + ")";

    new clojure.lang.RT(); // needed by Clojure
    return (OhuaRuntime) Compiler.load(new StringReader(code));
  }

  @SuppressWarnings("unchecked")
  @Test//(timeout = 30000)
  public void testBasic() throws Throwable {
    OhuaRuntime runtime = prepareProgram();

    AtomicReference<?> result = new AtomicReference<>(null);
    runtime.setArguments(100, new Tuple[]{new Tuple(1, result)});
    runtime.setArguments(102, new Tuple[]{new Tuple(0, IntStream.range(0, 10).boxed().collect(Collectors.toList()))});

    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    Properties properties = new Properties();
    properties.put("disable-fusion", true);
    config.setProperties(properties);
    runtime.execute(config);
    // smap produces a list
    Assert.assertTrue(result.get() instanceof List);
    Assert.assertEquals(324, ((List<Long>) result.get()).stream().mapToLong(Long::longValue).sum());
  }

  @Ignore // FIXME this does not work well with the strict call semantics
  @Test(timeout = 30000)
  public void testFusionForHeavyLoad() throws Throwable {
    OhuaRuntime runtime = prepareProgram();

    AtomicReference<?> result = new AtomicReference<>(null);
    runtime.setArguments(100, new Tuple[]{new Tuple(1, result)});
    runtime.setArguments(102, new Tuple[]{new Tuple(0, IntStream.range(0, 1000000).boxed().collect(Collectors.toList()))});

    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    Properties properties = new Properties();
//    properties.put("disable-fusion", true);
    config.setProperties(properties);
    runtime.execute(config);
  }

  @Ignore // FIXME this does not work well with the strict call semantics
  @Test(timeout = 30000)
  public void testOutputFused() throws Throwable {
    OhuaRuntime runtime = prepareProgram();

    AtomicReference<?> result = new AtomicReference<>(null);
    runtime.setArguments(100, new Tuple[]{new Tuple(1, result)});
    runtime.setArguments(102, new Tuple[]{new Tuple(0, IntStream.range(0, 1000000).boxed().collect(Collectors.toList()))});

    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    Properties properties = new Properties();
    properties.put("operator-fusion.input", false);
//    properties.put("disable-fusion", true);
    config.setProperties(properties);
    runtime.execute(config);
  }
}
