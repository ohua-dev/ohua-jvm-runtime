/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.util;

import clojure.lang.Indexed;

import java.util.function.Function;

/**
 * Created by sertel on 3/24/16.
 */
public class Tuple<S, T> implements Indexed {
  public S _s = null;
  public T _t = null;

  public Tuple(S s, T t) {
    _s = s;
    _t = t;
  }

  public S first() {
    return _s;
  }

  public T second() {
    return _t;
  }

  public boolean equals(Tuple<S, T> other) {
    return _s.equals(other._s) && _t.equals(other._t);
  }

  @Override
  public Object nth(int i) {
    if(i > 1)
      throw new IllegalArgumentException();
    return i == 0 ? _s : _t;
  }

  @Override
  public Object nth(int i, Object o) {
    Object a = nth(i);
    return a == null ? o : a;
  }

  @Override
  public int count() {
    return 2;
  }

  public static <S,T> Function<T,Tuple<S,T>> partialTuple1(S first) {
    return second -> new Tuple<S,T>(first,second);
  }

  public static <S,T> Function<S,Tuple<S,T>> partialTuple2(T second) {
    return first -> new Tuple<S,T>(first,second);
  }
}
