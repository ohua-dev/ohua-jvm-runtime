/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * This logger code is taken from SimpleFormatter with slight modifications.
 * @author sertel
 * 
 */
public class OhuaLoggingFormatter extends Formatter
{
  Date dat = new Date();
  private final static String format = "{0,date} {0,time}";
  private MessageFormat formatter;
  
  private Object args[] = new Object[1];
  
  // Line separator string. This is the value of the line.separator
  // property at the moment that the SimpleFormatter was created.
  private String lineSeparator = "\n";
//      java.security.AccessController.doPrivileged(new sun.security.action.GetPropertyAction("line.separator"));
  
  /**
   * Format the given LogRecord.
   * @param record the log record to be formatted.
   * @return a formatted log record
   */
  @Override
  public synchronized String format(LogRecord record)
  {
    StringBuffer sb = new StringBuffer();
    
    // first is the thread
    sb.append("Thread (" + Thread.currentThread().getName() + ") ");

    // Minimize memory allocations here.
    dat.setTime(record.getMillis());
    args[0] = dat;
    StringBuffer text = new StringBuffer();
    if(formatter == null)
    {
      formatter = new MessageFormat(format);
    }
    formatter.format(args, text, null);

    sb.append(text);
    sb.append(" ");
    if(record.getSourceClassName() != null)
    {
      sb.append(record.getSourceClassName());
    }
    else
    {
      sb.append(record.getLoggerName());
    }
    if(record.getSourceMethodName() != null)
    {
      sb.append(" ");
      sb.append(record.getSourceMethodName());
    }
    
    // we want to print this information all to one line. that makes filtering in log viewers
    // much easier
    // sb.append(lineSeparator);
    String message = formatMessage(record);
    sb.append(" [" + record.getLevel().getLocalizedName() + "]");
    sb.append(": ");
    sb.append(message);
    sb.append(lineSeparator);
    if(record.getThrown() != null)
    {
      try
      {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        record.getThrown().printStackTrace(pw);
        pw.close();
        sb.append(sw.toString());
      }
      catch(Exception ex)
      {
        // no problem, since we have already printed the stack trace above
      }
    }
    return sb.toString();
  }
}
