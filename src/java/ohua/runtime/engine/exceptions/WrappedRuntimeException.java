/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.exceptions;

/**
 * This exception is propagated throw the engine and rethrown into the application.
 * 
 * @author sertel
 *
 */
public class WrappedRuntimeException extends RuntimeException {
  public WrappedRuntimeException(Throwable t) {
    super(t);
  }
}
