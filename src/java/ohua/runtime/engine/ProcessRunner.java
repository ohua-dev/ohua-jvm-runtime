/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.IOException;
import java.util.function.Supplier;

import ohua.runtime.engine.flowgraph.DataFlowComposition;

public final class ProcessRunner extends AbstractProcessRunner {

  public ProcessRunner(String pathToFlow) {
    super(pathToFlow);
  }
  
  public ProcessRunner() {
    super((String) null);
  }
  
  public ProcessRunner(Supplier<DataFlowProcess> loader) {
    super(loader);
  }

  public ProcessRunner(Supplier<DataFlowProcess> loader, RuntimeProcessConfiguration config) {
    super(loader, config);
  }


  public void setManager(AbstractProcessManager manager) {
    _manager = manager;
  }
  
  @Override
  public void loadRuntimeConfiguration(String pathToRuntimeConfiguration) throws IOException,
                                                                         ClassNotFoundException
  {
    super.loadRuntimeConfiguration(pathToRuntimeConfiguration);
  }
  
  // FIXME this is a leak. we should never give the process manager away. (at least not to the
  // program executing the flow.)
  public AbstractProcessManager getProcessManager() {
    if(_manager == null) {
      initializeProcessManager();
    }
    
    return _manager;
  }
  
  public void run() {
    if(_manager == null) {
      initializeProcessManager();
    }
    initialize();
    try {
      _manager.initializeProcess();
      _manager.awaitSystemPhaseCompletion();

      _manager.runFlow();
      _manager.awaitSystemPhaseCompletion();

      _manager.tearDownProcess();
      _manager.awaitSystemPhaseCompletion();
    }
    catch(Throwable t) {
      throw new RuntimeException(t);
    }
  }
  

  public void finishComputation() {
    _manager.finishComputation();
  }
  
  @Override
  protected void initialize() {
    // nothing
  }
  
  public RuntimeProcessConfiguration getConfig() {
    return _config;
  }
  
}
