/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OperatorLogFormatter extends Formatter
{
  private String _opName = null;
  private String _opID = null;
  
  public OperatorLogFormatter(String opName, String opID)
  {
    _opName = opName;
    _opID = opID;
  }

  @Override
  public String format(LogRecord arg0)
  {
    return _opName + "(" + _opID + "): " + super.formatMessage(arg0) + "\n";
  }

}
