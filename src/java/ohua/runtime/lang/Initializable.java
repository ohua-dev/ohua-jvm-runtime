/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.lang;

/**
 * If an operator needs to initialize anything then it needs to implement this interface.
 * 
 * @author sertel
 * 
 */
public interface Initializable
{
  public void initialize();
}
