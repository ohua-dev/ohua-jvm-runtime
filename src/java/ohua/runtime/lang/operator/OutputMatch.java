/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import clojure.lang.Indexed;
import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.exceptions.Assertion;

import java.util.Iterator;
import java.util.List;

public final class OutputMatch {

  public enum MATCH_TYPE {
    SINGLE,
    MULTIPLE,
    ALL
  }

  private OutputMatch() {
    // hide the constructor. this class should never be instantiated.
  }

  protected static OutputMatcher<?> selectMatch(Class<?> returnType, MATCH_TYPE match) {
    final OutputMatcher<?> matcher = selectMatch(returnType);
    switch (match) {
      case SINGLE:
        if(matcher instanceof SingleOutputMatcher) return matcher;
        else{
          final MultiOutputMatcher m = (MultiOutputMatcher) matcher;
          return new OutputMatcher<Object>() {
            @Override
            public Object matchTyped(OutputPortControl outPort, int[] indexes, Object result) {
              return m.access(result, indexes[0]);
            }

            @Override
            public Object allInTyped(Object result) {
              throw new UnsupportedOperationException("Always use the match function!");
            }
          };
        }
      case MULTIPLE:
        return matcher;
      case ALL:
        return new OutputMatcher<Object>() {
          @Override
          public Object matchTyped(OutputPortControl outPort, int[] indexes, Object result) {
            return matcher.allIn(result);
          }

          @Override
          public Object allInTyped(Object result) {
            throw new UnsupportedOperationException("Always use the match function!");
          }
        };
      }
    Assertion.impossible();
    return null;
  }


  private static OutputMatcher<?> selectMatch(Class<?> returnType) {
    if (returnType.isArray()) {
      return new ArrayOutputMatcher();
    } else if (Indexed.class.isAssignableFrom(returnType)) {
      return new IndexedOutputMatcher();
    } else if (List.class.isAssignableFrom(returnType)) {
      return new ListOutputMatcher();
    } else if (Iterable.class.isAssignableFrom(returnType)) {
      return new IterableOutputMatcher();
    } else {
      return new SingleOutputMatcher();
    }
  }

  public interface OutputMatcher<T> {
    @SuppressWarnings("unchecked")
    default Object match(OutputPortControl outPort, int[] indexes, Object result) {
      return matchTyped(outPort, indexes, (T) result);
    }

    @SuppressWarnings("unchecked")
    default Object allIn(Object result) {
      return allInTyped((T) result);
    }

    Object matchTyped(OutputPortControl outPort, int[] indexes, T result);

    Object allInTyped(T result);
  }

  public static class SingleOutputMatcher implements OutputMatcher<Object> {
    @Override
    public Object matchTyped(OutputPortControl outPort, int[] indexes, Object result) {
      return result;
    }

    @Override
    public Object allInTyped(Object result) {
      return result;
    }
  }

  public static abstract class MultiOutputMatcher<T> implements OutputMatcher<T> {
    @Override
    public final Object matchTyped(OutputPortControl outPort, int[] indexes, T results) {
      // TODO creating these arrays over and over again is certainly expensive but I'm not sure how to avoid this.
      Object[] partialResult = new Object[indexes.length];
      for (int i = 0; i < indexes.length; i++)
        partialResult[i] = access(results, indexes[i]);
      return partialResult;
    }

    abstract protected Object access(T results, int index);
  }

  public static class ArrayOutputMatcher extends MultiOutputMatcher<Object[]> {
    @Override
    protected Object access(Object[] results, int index) {
      return results[index];
    }

    @Override
    public Object allInTyped(Object[] result) {
      return result;
    }
  }

  public static class IterableOutputMatcher extends MultiOutputMatcher<Iterable<?>> {
    /**
     * Random access to an iterable is by default expensive!
     */
    @Override
    protected Object access(Iterable<?> results, int index) {
      Iterator<?> it = results.iterator();
      for (int i = 0; i < index; i++)
        it.next();
      return it.next();
    }

    /**
     * This is also very expensive.
     */
    @Override
    public Object allInTyped(Iterable<?> result) {
      return result;
    }
  }

  public static class ListOutputMatcher extends MultiOutputMatcher<List<?>> {
    @Override
    protected Object access(List<?> results, int index) {
      return results.get(index);
    }

    @Override
    public Object allInTyped(List<?> result) {
      return result;
    }
  }

  public static class IndexedOutputMatcher extends MultiOutputMatcher<Indexed> {
    @Override
    protected Object access(Indexed results, int index) {
      return results.nth(index);
    }

    @Override
    public Object allInTyped(Indexed result) {
      return result;
    }
  }
}
