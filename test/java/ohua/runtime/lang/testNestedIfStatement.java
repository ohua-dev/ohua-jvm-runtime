/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import clojure.lang.Compiler;
import ohua.link.JavaBackendProvider;
import ohua.runtime.test.AbstractRegressionTestCase;
import ohua.util.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.StringReader;

public class testNestedIfStatement extends AbstractRegressionTestCase {
  @Before
  public void clearLinker() {
    clearCache();
    loadCoreOps();
  }

  @Ignore // FIXME We dont have `merge` right now
  @Test(timeout = 30000)
  public void testConditionMerge() throws Throwable {
//    GraphVisualizer.PRINT_FLOW_GRAPH = super.getTestMethodOutputDirectory() + "graph";
    registerFunc("produce", ClojureTestOps.TestProduceOperator.class.getDeclaredMethod("produce"));
    registerFunc("add", ClojureTestOps.AddOperator.class.getDeclaredMethod("add", int.class, long.class));
    registerFunc("subtract", ClojureTestOps.SubtractOperator.class.getDeclaredMethod("subtract", int.class, long.class));

    JavaBackendProvider.registerFunction("com.ohua.tests.lang", "collect", ClojureTestOps.TestCollectOperator.class.getDeclaredMethod("collect", long.class, long[].class));

    String code =
            "(doto (new ohua.runtime.lang.OhuaRuntime)"
                    + "(.createOperator \"" + testNS + "/produce\" 100)"
                    + "(.createOperator \"" + testNS + "/com.ohua.tests.lang/collect\" 101)"
                    + "(.createOperator \"merge\" 102)"
                    + "(.createOperator \"com.ohua.lang/ifThenElse\" 103)"
                    + "(.createOperator \"" + testNS + "/add\" 105)"
                    + "(.createOperator \"merge\" 106)"
                    + "(.createOperator \"com.ohua.lang/scope\" 107)"
                    + "(.createOperator \"merge\" 108)"
                    + "(.createOperator \"com.ohua.lang/ifThenElse\" 109)"
                    + "(.createOperator \"merge\" 111)"
                    + "(.createOperator \"com.ohua.lang/scope\" 112)"
                    + "(.createOperator \"" + testNS + "/add\" 113)"
                    + "(.createOperator \"merge\" 114)"
                    + "(.createOperator \"com.ohua.lang/scope\" 115)"
                    + "(.createOperator \"merge\" 116)"
                    + "(.createOperator \"com.ohua.lang/ifThenElse\" 117)"
                    + "(.createOperator \"merge\" 119)"
                    + "(.createOperator \"com.ohua.lang/scope\" 120)"
                    + "(.createOperator \"" + testNS + "/subtract\" 121)"
                    + "(.registerDependency 102 -1 101 0)"
                    + "(.registerDependency 105 -1 102 0)"
                    + "(.registerDependency 106 -1 102 1)"
                    + "(.registerDependency 108 -1 106 1)"
                    + "(.registerDependency 111 -1 108 0)"
                    + "(.registerDependency 113 -1 111 1)"
                    + "(.registerDependency 114 -1 108 1)"
                    + "(.registerDependency 116 -1 114 1)"
                    + "(.registerDependency 119 -1 116 0)"
                    + "(.registerDependency 121 -1 119 1)"
                    + "(.registerDependency 100 -1 103 1)"
                    + "(.registerDependency 100 -1 105 0)"
                    + "(.registerDependency 100 -1 107 0)"
                    + "(.registerDependency 107 0 109 1)"
                    + "(.registerDependency 107 0 112 0)"
                    + "(.registerDependency 112 0 113 0)"
                    + "(.registerDependency 107 0 115 0)"
                    + "(.registerDependency 115 0 117 1)"
                    + "(.registerDependency 115 0 120 0)"
                    + "(.registerDependency 120 0 121 0)"
                    + "(.registerDependency 103 0 105 -1)"
                    + "(.registerDependency 103 1 107 -1)"
                    + "(.registerDependency 109 0 112 -1)"
                    + "(.registerDependency 109 1 115 -1)"
                    + "(.registerDependency 117 0 120 -1)"
                    // +
                    // "(.setArguments 101 (clojure.core/into-array java.lang.Object '(result-merge)))"
                    + "(.setArguments 103 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 0) (clojure.core/reify com.ohua.lang.Condition (check [this args] (< (clojure.core/nth args 0) 3))) ))))"
                    + "(.setArguments 105 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 1) 100))))"
                    + "(.setArguments 109 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 0) (clojure.core/reify com.ohua.lang.Condition (check [this args] (< (clojure.core/nth args 0) 5))) ))))"
                    + "(.setArguments 113 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 1) 200))))"
                    + "(.setArguments 117 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 0) (clojure.core/reify com.ohua.lang.Condition (check [this args] (> (clojure.core/nth args 0) 4))) ))))"
                    + "(.setArguments 121 (clojure.core/into-array ohua.util.Tuple (list (ohua.util.Tuple. (clojure.core/int 1) 3))))"
                    + ")";
    new clojure.lang.RT(); // needed by Clojure
    OhuaRuntime runtime = (OhuaRuntime) Compiler.load(new StringReader(code));
    long[] result = new long[10];
    runtime.setArguments(101, new Tuple[]{new Tuple(1, result)});
    runtime.execute();
    long sum = 0;
    for (long i : result)
      sum += i;
    Assert.assertEquals(730, sum);
  }
}
