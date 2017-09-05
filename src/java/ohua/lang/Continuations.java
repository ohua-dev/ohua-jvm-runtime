/*
 * Copyright (c) Sebastian Ertel and Justus Adam 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import ohua.runtime.lang.operator.ContinuationRuntime;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by sertel on 9/12/16.
 */
public final class Continuations extends ContinuationRuntime.AbstractContinuations {

  private Continuations() {
    super();
  }

  private Continuations(Object result) {
    super(result);
  }

  public static Continuations empty() {
    return new Continuations();
  }

  public static Continuations finish(Object result) {
    return new Continuations(result);
  }

  public static <T> Function<T, Continuations> passThrough() {
    return Continuations::finish;
  }

  public <T> Continuations at(NonStrict<T> arg, Function<T, Continuations> f) {
    arg.freeze();
    if (_finished) {
      throw new IllegalStateException("Can't add to finished continuation.");
    } else {
      _conts.add(super.createContinuation(arg.get(), f));
      return this;
    }
  }

  public <T> Continuations consume(NonStrict<T> arg, Consumer<T> c) {
    arg.freeze();
    _conts.add(super.createConsumingContinuation(arg.get(), c));
    return this;
  }

  public Continuations discard(NonStrict<?> arg) {
    arg.freeze();
    return consume(arg, a -> {
    });
  }

  public Continuations handsOff(NonStrict<?>... args) {
    for (NonStrict<?> a : args) {
      a.freeze();
    }
    _finished = true;
    return this;
  }

  public Continuations done(Object result){
    _finished = true;
    _result = result;
    return this;
  }

}
