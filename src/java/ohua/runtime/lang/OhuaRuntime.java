/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.engine.*;
import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.exceptions.CompilationException;
import ohua.runtime.lang.operator.AbstractFunctionalOperator;
import ohua.util.Tuple;

import java.util.*;

public class OhuaRuntime extends OhuaProgram {
  private OhuaProcessRunner _runner = null;
  private SimpleProcessListener _listener = null;
  public static long EXEC_START = 0;
  
  public OhuaRuntime() {
    // TODO support multiple runtimes in a single clojure program
    OhuaRuntimeRegistry.get().register("single-runtime", this);
  }
  
  public void setArguments(int operator, Tuple<Integer, Object>[] arguments) throws CompilationException {
    super.setArguments(operator, arguments);
  }
  
  /**
   * Execute the flow using the default runtime configuration.
   */
  public void execute() throws Throwable {
    RuntimeProcessConfiguration.LOGGING_ENABLED = false;
    execute(new RuntimeProcessConfiguration());
  }
  
  public void execute(Map<String, Object> compileTimeInfo) throws Throwable {
    RuntimeProcessConfiguration config = handleCompileTimeInfo(new RuntimeProcessConfiguration(), compileTimeInfo);
    execute(config);
  }
  
  /**
   * The final call to run the current flow graph.
   */
  public void execute(RuntimeProcessConfiguration config) throws Throwable {
    PreparedRuntimeConfiguration conf = handleCompileTimeInfo(config, Collections.emptyMap());
    executeNoWait(conf);
    awaitCompletion();
  }

  public void execute(PreparedRuntimeConfiguration config) throws Throwable {
    executeNoWait(config);
    awaitCompletion();
  }

  public void execute(RuntimeProcessConfiguration config, Map<String, Object> compileTimeInfo) throws Throwable {
    PreparedRuntimeConfiguration conf = handleCompileTimeInfo(config, compileTimeInfo);
    execute(conf);
  }
  
  private PreparedRuntimeConfiguration handleCompileTimeInfo(RuntimeProcessConfiguration config,
                                                            Map<String, Object> compileTimeInfo)
  {
    // System.out.println("Received compile time info: " + compileTimeInfo);
    PreparedRuntimeConfiguration conf =
        new PreparedRuntimeConfiguration(getCompileTimeView(), getRuntimeView(), compileTimeInfo);
    config.aquirePropertiesAccess(conf);
    return conf;
  }
    
  /**
   * Execute the flow without waiting for its completion (using the default runtime
   * configuration).
   */
  public void executeNoWait() throws Throwable {
    PreparedRuntimeConfiguration conf = handleCompileTimeInfo(new RuntimeProcessConfiguration(), Collections.emptyMap());
    executeNoWait(conf);
  }
  
  public void executeNoWait(RuntimeProcessConfiguration config) throws Throwable {
    PreparedRuntimeConfiguration conf = handleCompileTimeInfo(config, Collections.emptyMap());
    executeNoWait(conf);
  }

  public void executeNoWait(PreparedRuntimeConfiguration config) throws Throwable {
//    System.out.println(config);
    super.prepare();
    config.prepare(_process.getGraph());

    RuntimeProcessConfiguration.LOGGING_ENABLED = config.isLoggingEnabled();
    
    if(RuntimeProcessConfiguration.LOGGING_ENABLED) System.out.println("Executing flow graph ...");
    _runner = new OhuaProcessRunner(this, config);
    
    _listener = new SimpleProcessListener();
    _runner.register(_listener);
    Thread runnerThread = new Thread(_runner, "ohua-process");
    runnerThread.setDaemon(true);
    runnerThread.start();
    _runner.submitUserRequest(new UserRequest(UserRequestType.INITIALIZE));
    _listener.awaitProcessingCompleted();
    _listener.reset();
    EXEC_START = System.currentTimeMillis();
    _runner.submitUserRequest(new UserRequest(UserRequestType.START_COMPUTATION));
  }
  
  public void awaitCompletion() throws Throwable {
    _listener.awaitProcessingCompleted();
    if(RuntimeProcessConfiguration.LOGGING_ENABLED) System.out.println("Exec only: " + (System.currentTimeMillis() - EXEC_START));
    _listener.reset();
    _runner.submitUserRequest(new UserRequest(UserRequestType.SHUT_DOWN));
    _listener.awaitProcessingCompleted();
    RuntimeStatistics.GlobalStatsLogger.log(_process.getGraph());
    if(RuntimeProcessConfiguration.LOGGING_ENABLED) System.out.println("Done!");
  }
  
  public RuntimeView getRuntimeView() {
    final OhuaRuntime runtime = this;
    return new RuntimeView() {
      @Override
      public List<String> getAllOperators(String regex) {
        List<OperatorCore> ops = _process.getGraph().getOperators(regex);
        List<String> opIds = new ArrayList<String>(ops.size());
        for(OperatorCore op : ops)
          opIds.add(op.getOperatorName());
        return opIds;
      }
      
      @Override
      public AbstractFunctionalOperator createOperator(String type) throws OperatorLoadingException {
        return (AbstractFunctionalOperator) runtime._process.getGraph().getOperatorFactory().createUserOperatorInstance(type);
      }

      @Override
      public OperatorCore findOperator(String opName) {
        return _process.getGraph().getOperator(opName);
      }

      @Override
      public OperatorCore findOperator(int id) {
        return runtime.findOperator(_process.getGraph(), id);
      }
    };
  }
  
  public void inject(LinkedList<IMetaDataPacket> requests) {
    UserRequest request = new UserRequest(UserRequestType.FLOW_INPUT, requests);
    _runner.submitUserRequest(request);
  }
}
