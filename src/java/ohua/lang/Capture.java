/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import java.util.concurrent.atomic.AtomicReference;

public class Capture {
  @defsfn
  public void capture(Object result, AtomicReference<Object> ref) {
    ref.set(result);
  }
}
