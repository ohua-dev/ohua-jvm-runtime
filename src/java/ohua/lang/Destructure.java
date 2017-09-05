/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

public class Destructure {
  @defsfn
  public Object[] destructure(Object data) {
//    System.out.println(Arrays.deepToString((Object[]) data));
    return (Object[]) data;
  }
}
