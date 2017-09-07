/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.IOException;
import java.util.function.Supplier;

public final class ProcessRunner extends AbstractProcessRunner {

  public ProcessRunner(AbstractProcessManager manager) {
    super(manager);
  }
  
  public ProcessRunner(Supplier<DataFlowProcess> loader) {
    super(loader);
  }

  public ProcessRunner(Supplier<DataFlowProcess> loader, RuntimeProcessConfiguration config) {
    super(loader, config);
  }

  @Override
  public void loadRuntimeConfiguration(String pathToRuntimeConfiguration) throws IOException,
                                                                         ClassNotFoundException
  {
    super.loadRuntimeConfiguration(pathToRuntimeConfiguration);
  }

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
