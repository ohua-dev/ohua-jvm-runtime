/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.DataFlowComposition;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;

/**
 * I think the communication to the user (as in the application that wants to run this Ohua
 * flow) should be like this:<br>
 * The user actually never knows what exact state the process is in. It better remember its last
 * start/stop request (and that could be even something that helps us identify what state the
 * process is in). In general it can only be in two states:<br>
 * <ul>
 * <li>IDLE - All processing has finished either due to the request of a user or because all the
 * data in the input source has been processed successfully.
 * <li>RUNNING - Processing data. This can mean that the process is really active or waiting for
 * one or many of its input sources to get more data.
 * </ul>
 * In order to steer the process and influence it's processing the user can issue various types
 * of requests:<br>
 * <ul>
 * <li>INITIALIZE - A request to initialize the process.
 * <li>START_COMPUTATION - A request to start processing the data from the sources of the flow.
 * <li>FINISH_COMPUTATION - A request to finish a user-driven (EAI or NRT) flow. All ETL flows
 * will ignore this request. Once computation is completed a notification is issued to the user.
 * <li>SHUT_DOWN - An indicator that now further computation is required and the process can
 * shut down. After this has finished successfully another notification will be issued and the
 * process will be destroyed.
 * <li>FLOW_INPUT - TBD this will be the point where the user can inject new input into a
 * running flow. This input can be many things: Configuration information for the load balancer,
 * new configuration information for an operator etc.
 * </ul>
 * @author sertel
 * 
 */
public final class OhuaProcessRunner extends AbstractProcessRunner {
  
  protected class Waiter implements Runnable {
    public void run() {
      Throwable t = null;
      try {
        _manager.awaitSystemPhaseCompletion();
      }
      catch(Throwable th) {
        t = th;
      }
      notifyListeners(new UserRequest(UserRequestType.FINISH_COMPUTATION), t);
    }
  }
  
  private ArrayBlockingQueue<UserRequest> _requests = new ArrayBlockingQueue<UserRequest>(10);
  
  /*
   * Listeners are being notified for main events; not for FLOW_INPUT.
   */
  private List<OhuaProcessListener> _listeners = new LinkedList<OhuaProcessListener>();
  
  public OhuaProcessRunner(DataFlowComposition loader) {
    super(loader);
  }
  
  public OhuaProcessRunner(DataFlowComposition loader, RuntimeProcessConfiguration config) {
    super(loader, config);
  }
  
  @Override
  protected void initialize() {
    initializeProcessManager();
  }
  
  @SuppressWarnings("unchecked")
  public void run() {
    // we just simply avoid concurrency on this list by making it unmodifiable during runtime.
    _listeners = Collections.unmodifiableList(_listeners);
    
    // enter the request loop
    boolean done = false;
    while(!done) {
      // blocks until requests become available
      UserRequest request = retrieveRequest();
      try {
        switch(request.getRequestType()) {
          case INITIALIZE:
            initialize();
            _manager.initializeProcess();
            _manager.awaitSystemPhaseCompletion();
//            _manager.runGraphAnalysisAlgorithms();
            request.submitted();
            _manager.awaitSystemPhaseCompletion();
            notifyListeners(request);
            break;
          case START_COMPUTATION:
            _manager.runFlow();
            new Thread(new Waiter()).start();
            request.submitted();
            notifyListeners(request);
            break;
          case FINISH_COMPUTATION:
            // this call is only effective for user-driven flows.
            _manager.finishComputation();
            request.submitted();
            break;
          case FLOW_INPUT:
            if(request.getInput() == null) {
              throw new IllegalArgumentException("Missing input: Flow input must not be null.");
            }
            _manager.submitFlowInput((LinkedList<IMetaDataPacket>) request.getInput());
            request.submitted();
            break;
          case SHUT_DOWN:
            // blocking call
            _manager.tearDownProcess();
            request.submitted();
            _manager.awaitSystemPhaseCompletion();
            // drop all other requests and finish this thread
            done = true;
            notifyListeners(request);
            break;
        }
      }
      catch(Throwable t) {
        notifyListeners(request, t);
      }
    }
  }
  
  private UserRequest retrieveRequest() {
    try {
      return _requests.take();
    }
    catch(InterruptedException e) {
      Assertion.impossible(e);
    }
    return null;
  }
  
  // FIXME A better API is to wait on the request instead of on the process runner!
  /**
   * Registers a listener for events of the process. Listeners can only be registered before
   * starting the runner!
   * @param listener
   * @throws UnsupportedOperationException - when the process is running.
   */
  public void register(OhuaProcessListener listener) {
    _listeners.add(listener);
  }
  
  // FIXME It feels like this should be the other way around. The request should know its
  // listeners!
  // This would also remove the need for another thread to be created on request submission.
  protected void notifyListeners(UserRequest request) {
    notifyListeners(request, null);
  }
  
  protected void notifyListeners(UserRequest request, Throwable t) {
    for(OhuaProcessListener listener : _listeners) {
      listener.completed(request, t);
    }
  }
  
  /**
   * Submit a user request into the process.<br>
   * This function symbolizes the steering wheel of the process.
   * @param request
   */
  public void submitUserRequest(UserRequest request) {
    _requests.add(request);
  }
  
}
