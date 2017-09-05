/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.sections;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.logging.Logger;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.WrappedRuntimeException;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.operators.system.UserGraphExitOperator;

public class SectionTask extends FutureTask<SectionScheduler.Activation>
{
  
  protected Logger _logger = Logger.getLogger(getClass().getCanonicalName());
  
  protected NotificationBasedSectionRuntime _sectionRuntime = null;
  SectionScheduler _sectionScheduler = null;

  private long _start = -1;
  private long _finished = -1;

  public SectionTask(NotificationBasedSectionRuntime callable, SectionScheduler scheduler)
  {
    super(callable);
    _sectionRuntime = callable;
    _sectionScheduler = scheduler;
  }
  
  @Override public void run()
  {
    if(!canSectionRun())
    {
      // this section is already/still being executed.
      cancel(false);
      return;
    }

    if(RuntimeProcessConfiguration.LOGGING_ENABLED)
    {
      _logger.info("starting section: " + _sectionRuntime.toString());
    }
    beforeExecution();
    
    super.run();
  }
  
  protected boolean canSectionRun()
  {
    return _sectionRuntime.runSection();
  }
  
  protected void beforeExecution()
  {
    _sectionScheduler.beforeExecution(_sectionRuntime);
    _start = System.nanoTime();
  }
  
  // FIXME this is very problematic because whenever exceptions happen in the below code, they
  // will not be reported and just swallowed!
  @Override public void done()
  {
    super.done();

    // FIXME This is a bit tricky. If we counted this one into the running section count then we
    // at least have to report back to the scheduler that this section finished! (Otherwise we
    // will run into a deadlock!)
    if(isCancelled())
    {
      return;
    }

    Assertion.invariant(isDone());
    /*
     * this (potentially) activates all foreign sections. if the current section is in the list
     * of sections to be activated then it receives a notification and gets scheduled after this
     * function has finished in afterExecution().
     */
    int activated = activateResults();
    
    handleCompletedSystemPhase(_sectionRuntime._section);
    
    if(RuntimeProcessConfiguration.LOGGING_ENABLED)
    {
      _logger.info("finishing section: " + _sectionRuntime.toString());
    }

    afterExecution(activated);
  }
  
  protected void afterExecution(int activated)
  {
    _finished = System.nanoTime();
    SectionScheduler.FinishedSectionExecution finished =
            new SectionScheduler.FinishedSectionExecution(_sectionRuntime,
                    activated, _start, _finished,
                    _sectionRuntime._executed, _sectionRuntime._result);
    boolean free = _sectionScheduler.afterExecution(finished);
    if(free)
    {
      if(_sectionRuntime.freeSection())
      {
        // this call must succeed no matter what!
        _sectionScheduler.schedule(_sectionRuntime, 0);
//        try {
//          SectionScheduler.Activation a = new SectionScheduler.Activation((Section) _sectionRuntime);
//          a._isPendingNotification = true;
//          a._downStreamActivations = _sectionRuntime.getOperators().stream()
//                .filter(OperatorCore::isActive)
//                  .collect(Collectors.toSet());
//          a._upStreamActivations = Collections.EMPTY_SET;
//          _sectionScheduler.activateSection(a);
//        }catch(Exception e){
//          System.err.println(e.getMessage());
//          e.printStackTrace();
//          throw e;
//        }

      }
    }
  }
  
  protected int activateResults()
  {
    try
    {
      SectionScheduler.Activation activate = get();
      return _sectionScheduler.activateSections(activate);
    }
    catch(ExecutionException e)
    {
//      System.err.println("FATAL ERROR: shutting down system");
//      e.printStackTrace();
      Throwable t = e.getCause();
      if(t instanceof WrappedRuntimeException)
        t = t.getCause();
      _sectionScheduler.terminate(t);
      return 0;
      // we enter this function only if we are done!
      // Assertion.impossible(e);
    }
    catch(InterruptedException ie)
    {
      // we normally don't interrupt unless triggered by another crash
      Assertion.impossible();
      return 0;
    }
  }
  
  /**
   * This function will find the very last section that calls back and notify the section
   * scheduler that we are done.
   * @param finished
   * @return
   */
  private void handleCompletedSystemPhase(Section finished)
  {
    OperatorCore firstOp = finished.getOperators().get(0);
    if(firstOp.getOperatorAlgorithm() instanceof UserGraphExitOperator)
    {
      if(((UserGraphExitOperator) firstOp.getOperatorAlgorithm()).systemPhaseCompleted())
      {
        _sectionScheduler.done(finished);
      }
    }
  }
  
}
