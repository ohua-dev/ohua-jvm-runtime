/*
 * Copyright (c) Sebastian Ertel and Justus Adam 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import ohua.runtime.lang.operator.ContinuationRuntime;
import ohua.runtime.lang.operator.DataflowSchemaMatching;

import java.util.function.Supplier;

/**
 * Created by sertel on 9/12/16.
 */
public final class NonStrict<T> extends DataflowSchemaMatching.AbstractNonStrict{

  private ContinuationRuntime.PendingValue<T> _pending = null;
  private boolean _frozen = false;

  public NonStrict(Supplier<T> retrieval, Supplier<Boolean> check) {
    _pending = ContinuationRuntime.createPendingValue(retrieval, check);
  }

  ContinuationRuntime.PendingValue<T> get(){
    return _pending;
  }

  void freeze() {
    if (_frozen) throw new DuplicateContinuationException();
    _frozen = true;
  }

  protected DataflowSchemaMatching.AbstractNonStrict reset(){
    _frozen = false;
    // resuse the pending value object
    return this;
  }

  public static class DuplicateContinuationException extends RuntimeException {
    public DuplicateContinuationException() {
      super("Can't define more than one continutation for the same NonStrict.");
    }
  }


}
