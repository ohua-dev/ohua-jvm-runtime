/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by sertel on 9/5/16.
 */
public class Flatten {

  @defsfn
  public <T> Iterable<T> flatten(Iterable<Iterable<T>> t){
    return StreamSupport.stream(t.spliterator(), false).
            flatMap(ts -> StreamSupport.stream(ts.spliterator(), false)).
            collect(Collectors.toList());
  }
}
