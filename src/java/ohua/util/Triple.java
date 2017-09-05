/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.util;

/**
 * Created by sertel on 3/24/16.
 */
public class Triple<R,S,T> extends Tuple<S,T>{
  public R _r = null;
  public Triple(R r,S s, T t){
    super(s,t);
    _r = r;
  }

  @Override
  public Object nth(int i) {
    if(i > 2)
      throw new IllegalArgumentException();
    return i == 2 ? _r : super.nth(i);
  }

  @Override
  public int count() {
    return super.count() + 1;
  }

}
