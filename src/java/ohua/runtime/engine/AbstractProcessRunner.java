/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.File;
import java.io.IOException;

import ohua.runtime.engine.flowgraph.DataFlowComposition;
import ohua.runtime.engine.utils.parser.OhuaFlowParser;
import ohua.runtime.engine.utils.parser.ProcessConfigurationLoader;

public abstract class AbstractProcessRunner implements Runnable
{
  protected RuntimeProcessConfiguration _config = new RuntimeProcessConfiguration();
  protected AbstractProcessManager _manager = null;
  
  private DataFlowComposition _parser = null;

  public AbstractProcessRunner(String pathToFlow)
  {
    _parser = new OhuaFlowParser(pathToFlow);
  }

  public AbstractProcessRunner(DataFlowComposition loader)
  {
    _parser = loader;
  }

  public AbstractProcessRunner(DataFlowComposition loader, RuntimeProcessConfiguration config)
  {
    _parser = loader;
    _config = config;
  }

  public void loadRuntimeConfiguration(String pathToRuntimeConfiguration) throws IOException,
                                                                                  ClassNotFoundException
  {
    _config = ProcessConfigurationLoader.load(new File(pathToRuntimeConfiguration));
  }

  protected final void initializeProcessManager()
  {
    DataFlowProcess process = deserializeFlow();
    _manager = _config.getProcessManager(process);
  }

  private DataFlowProcess deserializeFlow()
  {
    DataFlowProcess process = null;
    try
    {
      process = _parser.load();
    }
    catch(Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return process;
  }
  
  abstract protected void initialize();

}
