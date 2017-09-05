/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * Buffers the output. BEWARE only use for loggers with small output, otherwise OutOfMemory
 * errors possible!
 * @author sertel
 * 
 */
public class OhuaBufferedLogHandler extends Handler
{
  private StringBuffer _buffer = new StringBuffer();

  @Override
  public void close()
  {
    _buffer = null;
  }
  
  @Override
  public void flush()
  {
    _buffer = new StringBuffer();
  }
  
  @Override
  public void publish(LogRecord record)
  {
    _buffer.append(record.getMessage());
  }
  
  public final StringBuffer getBuffer()
  {
    return _buffer;
  }
  
}
