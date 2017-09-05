/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * Prints to standard output!
 * @author sertel
 *
 */
public class OhuaConsoleLogHandler extends Handler
{
  
  @Override
  public void close() throws SecurityException
  {
    // nothing
  }
  
  @Override
  public void flush()
  {
    // nothing
  }
  
  @Override
  public void publish(LogRecord arg0)
  {
    System.out.println(arg0.getMessage());
  }
  
}
