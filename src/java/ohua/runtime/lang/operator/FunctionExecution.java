/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.exceptions.WrappedRuntimeException;

public class FunctionExecution {
  
  protected Object execute(String opName, StatefulFunction algorithm, Object[] actuals) {
    try {
      return algorithm.invoke(actuals);
    }
    catch(Throwable e) {
      // this exception wraps an exception that was thrown inside the function invocation, so we
      // have to extract it and rethrow.
//      Throwable t = e.getCause();
      if(e instanceof Error) {
        throw (Error) e;
      } else {
        throw new WrappedRuntimeException(e);
      }
    }
  }
}
