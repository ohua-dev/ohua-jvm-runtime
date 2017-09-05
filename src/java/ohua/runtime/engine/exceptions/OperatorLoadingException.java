/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.exceptions;


public class OperatorLoadingException extends Exception
{
  
  public OperatorLoadingException(Throwable e)
  {
    super(e);
  }

  public OperatorLoadingException(String string)
  {
    super(string);
  }

}
