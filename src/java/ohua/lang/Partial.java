/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import java.util.Arrays;

public class Partial {

  @defsfn
  public PartialFunction partial(Object function, Object... args) {
    return function instanceof PartialFunction ?
            ((PartialFunction) function).append(args) :
            new PartialFunction(function, args);
  }
  
  public static class PartialFunction extends Apply {
    private Object _fn = null;
    private Object[] _args = null;

    PartialFunction(Object fn, Object[] args) {
      _fn = fn;
      _args = args;
    }

    public Object apply(Object... args) throws Throwable {
      return super.apply(_fn, concat(_args, args));
    }

    private Object[] concat(Object[] first, Object[] second) {
      Object[] result = Arrays.copyOf(first, first.length + second.length);
      System.arraycopy(second, 0, result, first.length, second.length);
      return result;
    }

    private PartialFunction append(Object[] args) {
      _args = concat(_args, args);
      return this;
    }
  }
}
