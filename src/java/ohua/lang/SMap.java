/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import ohua.runtime.lang.operator.DataflowFunction;

import java.util.Iterator;

public class SMap {

  public static class InMemory {
    private Iterator<?> _it = null;

    @defsfn
    public Object[] smapFun(Iterable<?> data) {
      if (_it == null) _it = data.iterator();

      if (_it.hasNext()) {
        Object d = _it.next();
        return new Object[]{d};
      } else {
        // FIXME this is an endless loop if the iterable is empty!
        _it = null;
        return smapFun(data);
      }
    }
  }

  /**
   * Latency and throughput requirements are defined only via the scheduler, not via a function anymore!
   */
  public static class IO {
    private Iterator<?> _it = null;
    private int _size = 0;

    @DataflowFunction
    @defsfn
    public Either.EitherObjectArrayOrFinish smapIoFun(Iterable<?> col) {
      if (_it == null) _it = col.iterator();

      if (_it.hasNext()) {
        Object o = _it.next();
        _size++;
        return _it.hasNext() ?
                new Either.EitherObjectArrayOrFinish().add(new Object[]{o, DataflowFunction.Control.DROP}) :
                new Either.EitherObjectArrayOrFinish().add(new Object[]{o, _size}); // final emission of this collection

      } else {
        return new Either.EitherObjectArrayOrFinish().add(DataflowFunction.Finish.DONE);
      }
    }
  }
}
