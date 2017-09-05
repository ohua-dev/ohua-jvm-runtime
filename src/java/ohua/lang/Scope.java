/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

/**
 * Function that is inserted by a transformation step in the compiler to realize scoping of
 * local variables in conditions.
 * 
 * @author sertel
 *
 */
public class Scope {
  @defsfn
  public Object[] scope(Object... localVars){
    return localVars;
  }
}
