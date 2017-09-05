/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.exceptions;

public class InvariantBroken extends AssertionError
{
  
  public InvariantBroken(String string)
  {
    super(string);
  }

  public InvariantBroken()
  {
    //
  }
  
  public InvariantBroken(Exception cause)
  {
    super(cause);
  }

}
