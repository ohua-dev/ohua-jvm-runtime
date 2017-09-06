/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import ohua.runtime.engine.flowgraph.DataFlowComposition;
import ohua.runtime.engine.flowgraph.elements.AbstractUniqueID;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;
import ohua.runtime.engine.utils.parser.OhuaFlowParser;
import ohua.runtime.engine.utils.parser.ProcessConfigurationLoader;

public abstract class AbstractProcessRunner implements Runnable
{
  RuntimeProcessConfiguration _config = new RuntimeProcessConfiguration();
  AbstractProcessManager _manager = null;
  
  private Supplier<DataFlowProcess> _parser = null;

  public AbstractProcessRunner(AbstractProcessManager manager){
    _manager = manager;
  }

  @Deprecated
  public AbstractProcessRunner(String pathToFlow)
  {
    _parser = new OhuaFlowParser(pathToFlow);
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
    _config = ProcessConfigurationLoader.load(new File(pathToRuntimeConfiguration));
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

}
