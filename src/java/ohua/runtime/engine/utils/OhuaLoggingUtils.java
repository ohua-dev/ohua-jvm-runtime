/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public abstract class OhuaLoggingUtils
{
  public static class FileOutputFormatter extends Formatter
  {
    @Override
    public String format(LogRecord r)
    {
      return r.getMessage() + "\n";
    }
  }
  
  /*
   * When switching to MAC OS X I found out that the loggers apparently are being recreated
   * during runtime. That means the logger will also lose its reference to the file handler and
   * nothing will be printed to the file at all. In order to avoid that we just keep a reference
   * here and "magically" it will not be garbage collected. (Probably because now the LogManager
   * is not being garbage collected.)
   */
  private static List<Logger> _regressionLoggers = new ArrayList<Logger>();

  public static final FileHandler outputLogToFile(String loggerName,
                                                  Level level,
                                                  String targetDirectory) throws IOException
  {
    return outputLogToFile(OhuaLoggerFactory.getLogger(loggerName), level, targetDirectory);
  }
  
  public static final FileHandler outputLogToFile(Logger logger,
                                                  Level level,
                                                  String targetDirectory) throws IOException
  {
    FileHandler writer =
        createLogFileHandler(getLoggerName(logger.getName()), level, targetDirectory);

    logger.addHandler(writer);
    // please see the comment on the field for the purpose of this save routine
    _regressionLoggers.add(logger);
    return writer;
  }
  
  public static FileHandler createLogFileHandler(String loggerName,
                                                 Level level,
                                                 String targetDirectory) throws IOException
  {
    String filePath = targetDirectory + "/" + loggerName + ".log";
    File file = new File(filePath);
    if(!file.exists())
    {
      file.createNewFile();
    }
    FileHandler writer = new FileHandler(filePath);
    writer.setLevel(level);
    writer.setFormatter(new FileOutputFormatter());
    return writer;
  }

  private static String getLoggerName(String name)
  {
    int index = name.lastIndexOf(".");
    if(index > 0 && name.length() > index + 1)
    {
      return name.substring(index + 1);
    }
    else
    {
      return name;
    }
  }

}
