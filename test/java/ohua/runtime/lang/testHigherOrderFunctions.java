/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.lang.operator.StatefulFunction;
import ohua.lang.Apply;
import ohua.lang.Partial;
import ohua.lang.defsfn;
import ohua.runtime.test.AbstractFlowTestCase;
import org.junit.Assert;
import org.junit.Test;

public class testHigherOrderFunctions extends AbstractFlowTestCase {
  
  @Test
  public void testApply() throws Throwable {
    Apply a = new Apply();
    Object result = a.apply(StatefulFunction.resolve(new AddOperator()), 1,
            2L);

    Assert.assertEquals(3L, result);
  }

  @Test
  public void testPartial() throws Throwable {
    Partial p = new Partial();
    Partial.PartialFunction r = p.partial(StatefulFunction.resolve(new AddOperator()), 1);

    Apply a = new Apply();
    Object result = a.apply(r, 2L);
    Assert.assertEquals(3L, result);
  }
  
  @Test
  public void testPartialOfPartial() throws Throwable {
    Partial p = new Partial();
    Partial.PartialFunction r = p.partial(StatefulFunction.resolve(new ThreeAddOperator()), 1);

    p = new Partial();
    r = p.partial(r, 2);

    Apply a = new Apply();
    Object result = a.apply(r, 3);
    Assert.assertEquals(6L, result);
  }

  public static class AddOperator {
    @defsfn
    public long add(int value, long s) {
      return value + s;
    }
  }

  public static class ThreeAddOperator {
    @defsfn
    public long add(int one, int two, int three) {
      return one + two + three;
    }
  }
  
}
