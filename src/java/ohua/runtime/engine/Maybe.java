/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

import java.util.function.Function;

/**
 * Created by sertel on 12/19/16.
 */
// The value inside Optional can not be reset and therefore we would have to create a lot of Optional instances.
public class Maybe<T> {

  private enum Empty{
    EMPTY
  }

  private T _data = null;
  private boolean _empty = true;

  private Maybe() {
    empty(this);
  }

  public static Maybe empty() {
    return new Maybe();
  }

  public static Maybe empty(Maybe maybe) {
    // this would have been the right way to do it but it is expensive because it creates a new lambda every time.
//    return maybe.flatMap(oldValue -> {
      maybe._data = Empty.EMPTY;
      maybe._empty = true;
      return maybe;
//    });
  }

  public static <T> Maybe value(Maybe maybe, T value) {
    // this would have been the right way to do it but it is expensive because it creates a new lambda every time.
//    return maybe.flatMap(oldValue -> {
      maybe._data = value;
      maybe._empty = false;
      return maybe;
//      });
  }

  public static Maybe value(Object value) {
    Maybe m = new Maybe();
    m._data = value;
    m._empty = false;
    return m;
  }

  public <R> Maybe<R> flatMap(Function<T, Maybe<R>> f) {
    return f.apply(_data);
  }

  public boolean isPresent() {
    return !_empty;
  }

  public T get() {
    return _data;
  }

}
