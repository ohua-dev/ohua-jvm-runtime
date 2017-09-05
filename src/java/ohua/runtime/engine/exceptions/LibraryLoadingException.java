/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.exceptions;


public class LibraryLoadingException extends Exception
{ 
  public enum LIB_LOADING_FAILURE{
    LIB_NOT_FOUND
  }
  
  private LIB_LOADING_FAILURE _cause = null;
  
  public LibraryLoadingException(LIB_LOADING_FAILURE cause){
    _cause = cause;
  }
  
  public LIB_LOADING_FAILURE getCauze(){
    return _cause;
  }
}
