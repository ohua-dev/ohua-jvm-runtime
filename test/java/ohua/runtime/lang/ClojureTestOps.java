/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.lang.operator.DataflowFunction;
import ohua.lang.Either;
import ohua.lang.defsfn;

import java.util.List;
import java.util.Map;

public abstract class ClojureTestOps {
  public static class TestProduceOperator {
    int calls = 0;

    @DataflowFunction
    @defsfn
    public EitherIntOrControl produce() {
      if (calls >= 10) {
        return new EitherIntOrControl().add(DataflowFunction.Finish.DONE);
      } else {
//        System.out.println("Called TestProducerOperator: " + calls);
        return new EitherIntOrControl().add(calls++);
      }
    }

    public class EitherIntOrControl extends Either<Integer, DataflowFunction.Finish, EitherIntOrControl> {}
  }

  public static class AddOperator {
    @defsfn
    public long add(int value, long s) {
//      System.out.println("Called AddOperator: " + value + " + " + s + " = " + (value + s));
      return value + s;
    }
  }

  public static class SubtractOperator {
    @defsfn
    public long subtract(int value, long s) {
//      System.out.println("Called SubtractOperator: " + value + " - " + s + " = " + (value - s));
      return value - s;
    }
  }

  public static class TestCollectOperator {
    private int _index = 0;

    @defsfn
    public Object[] collect(long value, long[] values) {
      values[_index++] = value;
//      System.out.print("Called TestCollectOperator: ");
//      for (long l : values) {
//        System.out.print(l + ",");
//      }
//      System.out.println("");
      return new Object[]{};
    }
  }

  public static class TestConsumeOperator {
    @defsfn
    public Object[] consume(int value, int[] capture) {
      capture[0] = value;
      return new Object[]{};
    }
  }

  public static class Access {
    @defsfn
    public Object[] get(String key, Map<String, Object> ds) {
      Object value = release(key, ds);
      return new Object[]{key,
              value};
    }

    private Object release(String key, Map<String, Object> ds) {
      return ds.get(key);
    }
  }

  public static class Update {
    @defsfn
    public Object[] replace(String key, String value, Map<String, Object> ds) {
      return new Object[]{key,
              ds.put(key, value)};
    }
  }

  public static class ListCollect {
    @defsfn
    public Object[] lCollect(Object value, List<Object> output) {
      output.add(value);
      return new Object[0];
    }
  }

  public static class Pipe {
    private int _idx = 0;

    @DataflowFunction
    @defsfn
    public Either.EitherObjectOrFinish pipe(Object[] values) {
      if (_idx < values.length) return new Either.EitherObjectOrFinish().add(values[_idx++]);
      else return new Either.EitherObjectOrFinish().add(DataflowFunction.Finish.DONE);
    }
  }

}
