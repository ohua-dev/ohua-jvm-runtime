/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.lang;

import ohua.lang.defsfn;
import ohua.link.JavaBackendProvider;
import ohua.runtime.test.AbstractFlowTestCase;
import ohua.util.Tuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class testExceptions extends AbstractFlowTestCase {

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testRuntimeExceptionInFunction() throws Throwable {
    registerFunc("func-prod", testIfThenElseOperator.MultiProducer.class.getDeclaredMethod("produce", List.class));
    registerFunc("fail", Failure.class.getDeclaredMethod("fail", String.class));
    JavaBackendProvider.loadCoreOperators();

    OhuaRuntime runtime = new OhuaRuntime();
    runtime.createOperator(testNS + "/func-prod", 100);
    runtime.createOperator(testNS + "/fail", 102);

    runtime.registerDependency(100, 0, 102, 0);

    List<Object[]> input = new ArrayList<>();
    input.add(new Object[]{"some"});
    runtime.setArguments(100, new Tuple[]{new Tuple(0, input)});

    runtime.execute();
  }

  public static class Failure {
    @defsfn
    public Object[] fail(String arg) {
      throw new ArrayIndexOutOfBoundsException();
    }
  }
}
