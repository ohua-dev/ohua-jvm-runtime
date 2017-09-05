/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * We can not use a formatter for this job, because in the end all loggers will print also to
 * the console and we want them to print the log entry in the same format to the console as they
 * print it to the operator log.
 * @author sertel
 * 
 */
public class OperatorLogger extends Logger
{
  private String _opName = null;
  private String _opID = null;
  private Logger _logger = null;
  
  protected OperatorLogger(Logger logger, String opName, String opID)
  {
    super(logger.getName(), logger.getResourceBundleName());
    _logger = logger;
    _opName = opName;
    _opID = opID;
  }
  
  @Override
  public void log(LogRecord record)
  {
    record.setMessage(_opName + "(" + _opID + "): " + record.getMessage());
    _logger.log(record);
  }
}
