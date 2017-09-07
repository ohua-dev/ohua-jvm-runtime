/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;

public abstract class AbstractProcessRunner implements Runnable
{
  RuntimeProcessConfiguration _config = new RuntimeProcessConfiguration();
  AbstractProcessManager _manager = null;
  
  private Supplier<DataFlowProcess> _parser = null;

  public AbstractProcessRunner(AbstractProcessManager manager){
    _manager = manager;
  }

  public AbstractProcessRunner(Supplier<DataFlowProcess> loader)
  {
    _parser = loader;
  }

  public AbstractProcessRunner(Supplier<DataFlowProcess> loader, RuntimeProcessConfiguration config) {
    _parser = loader;
    _config = config;
  }

  public void loadRuntimeConfiguration(String pathToRuntimeConfiguration) throws IOException,
                                                                                  ClassNotFoundException {
    _config = load(new File(pathToRuntimeConfiguration));
  }

  public void addProperty(String key, Object value){
    _config._properties.put(key, value);
  }

  final void initializeProcessManager() {
    DataFlowProcess process = deserializeFlow();
    process.getGraph().repopulateOperatorRegistry();
    _manager = _config.getProcessManager(process);
  }

  private DataFlowProcess deserializeFlow() {
    return _parser.get();
  }
  
  abstract protected void initialize();

  private RuntimeProcessConfiguration load(File processConfiguration) throws IOException,
          ClassNotFoundException {
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
    catch(IllegalAccessException | InstantiationException e)
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
