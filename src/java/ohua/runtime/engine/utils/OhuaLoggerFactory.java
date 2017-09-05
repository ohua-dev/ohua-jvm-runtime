/*
 * Copyright (c) Sebastian Ertel 2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.util.logging.Handler;
import java.util.logging.Logger;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

/**
 * 
 * @author sertel
 * 
 */
public class OhuaLoggerFactory
{
  public static boolean printToSysout = false;

  public static Logger getLogger(Class<?> cls)
  {
    return getLogger(cls.getCanonicalName());
  }
  
  public static Logger getLogger(String loggerName)
  {
    Logger logger = Logger.getLogger(loggerName);
    if(printToSysout)
    {
      logger.addHandler(new OhuaConsoleLogHandler());
    }
    return logger;
  }
  
  public static Logger getLogger(String loggerID, OperatorLogFormatter operatorLogFormatter)
  {
    Logger logger = getLogger(loggerID);
    for(Handler handler : logger.getHandlers())
    {
      handler.setFormatter(operatorLogFormatter);
    }
    return logger;
  }

  public static Logger getOperatorLogger(Class<?> clz, String operatorName, OperatorID id)
  {
    return getOperatorLogger(clz, operatorName, Integer.toString(id.getIDInt()));
  }
  
  public static Logger getOperatorLogger(Class<?> clz, String operatorName, String id)
  {
    Logger logger = getLogger(getLogIDForOperator(clz, operatorName, id));
    return logger;
  }
  
  public static String getLogIDForOperator(Class<?> clz, String operatorName, String opID)
  {
    return clz.getCanonicalName() + "." + operatorName + "-" + opID;
  }

}
