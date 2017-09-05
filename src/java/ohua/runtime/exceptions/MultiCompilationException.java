/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.exceptions;

import java.util.List;

public class MultiCompilationException extends CompilationException {
  
  private List<CompilationException> _failed = null;
  
  public MultiCompilationException(List<CompilationException> failed) {
    super(CAUSE.EXCEPTION_COLLECTION);
    _failed = failed;
  }
  
  public String getOpId() {
    throw new UnsupportedOperationException();
  }
  
  public List<CompilationException> getFailures() {
    return _failed;
  }
  
  public String getMessage() {
    String msg = super.getMessage();
    StringBuffer buf = new StringBuffer();
    buf.append(msg);
    buf.append("\n");
    for(CompilationException fail : _failed){
      buf.append(fail.getMessage());
      buf.append("\n");
    }
    return buf.toString();
  }
  
}
