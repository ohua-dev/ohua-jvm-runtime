/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils.parser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import ohua.runtime.engine.RuntimeProcessConfiguration;

public class ProcessConfigurationLoader
{
  public static RuntimeProcessConfiguration load(File processConfiguration) throws IOException,
                                                                            ClassNotFoundException
  {
    Properties properties = new Properties();
    FileReader reader = new FileReader(processConfiguration);
    properties.load(reader);
    reader.close();
    
    String name =
        properties.getProperty("runtime-properties-class",
                               "ohua.runtime.engine.RuntimeProcessConfiguration");
    Object runtimeProperties = null;
    try
    {
      runtimeProperties = Class.forName(name).newInstance();
    }
    catch(IllegalAccessException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(InstantiationException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    
    if(runtimeProperties instanceof RuntimeProcessConfiguration)
    {
      ((RuntimeProcessConfiguration) runtimeProperties).setProperties(properties);
    }
    else
    {
      throw new RuntimeException("Unknown runtime properties class.");
    }
    
    return (RuntimeProcessConfiguration) runtimeProperties;
  }
}
