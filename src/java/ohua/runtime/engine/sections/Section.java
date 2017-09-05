/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

public class Section extends AbstractSection//<OperatorScheduler>
{
//  private AtomicBoolean _running = new AtomicBoolean(false);
//  private AtomicBoolean _scheduled = new AtomicBoolean(false);
//  private AtomicBoolean _notificationPending = new AtomicBoolean(false);
//
//  // analysis state
//  public SchedulerResult _result;
//  public boolean _executed = false;
//
//  /**
//   * When the section is not "running" yet, then this method will return success (true).
//   * @return
//   */
//  protected boolean runSection()
//  {
//    boolean success = _running.compareAndSet(false, true);
//    if(success)
//    {
////      Assertion.invariant(_scheduled.get());
//      // notifications are cleared only here!
//      _notificationPending.set(false);
//    }
//    else
//    {
//      // enqueue a notification (just to be on the save side)
//      _notificationPending.set(true);
//    }
//    return success;
//  }
//
//  protected boolean freeSection()
//  {
//    _running.set(false);
//    // don't even unlock when there are notifications pending
//    _scheduled.set(_notificationPending.get());
//    return activateOnDemand();
//  }
//
//  private void issueNotification()
//  {
//    _notificationPending.set(true);
//  }
//
//  /**
//   * This function is used when another section wants to know whether it can activate this
//   * section. If it is not yet scheduled then it gets permission to do so.
//   * @return
//   */
//  protected boolean canActivate()
//  {
//    // always leave a notification
//    issueNotification();
//    return _scheduled.compareAndSet(false, true);
//  }
//
//  /**
//   * This method is used only by this section (or its thread) to check after execution whether
//   * it needs to reschedule itself. When it has a notification pending then this is the case.
//   * @return
//   */
//  private boolean activateOnDemand()
//  {
//    return _notificationPending.get();
//  }
//
//  // FIXME I would probably rather see this being done via applyState().
//
//  /**
//   * A function that resets the running state of the section such that it is free to run again.
//   * We keep the notifications though.
//   * <p>
//   * WARNING: Use this function only when the operator is already executing and it is clear that
//   * this state is not changed concurrently!
//   */
//  public void pureFree()
//  {
//    _running.set(false);
//    _scheduled.set(false);
//  }
//
//  /**
//   * A function that simply which to know if the section is scheduled already and if not
//   * schedule it.
//   * <p>
//   * WARNING: Use this function only when the operator is already executing and it is clear that
//   * this state is not changed concurrently!
//   */
//  protected boolean pureBlock()
//  {
//    _running.set(true);
//    return _scheduled.compareAndSet(false, true);
//  }
  
//  public void startNewSystemPhase()
//  {
//    super.startNewSystemPhase();
//    pureFree();
//    _notificationPending.set(false);
//  }
//
//  @Override
//  protected final SectionScheduler.Activation runSectionCycle()
//  {
//    if(RuntimeProcessConfiguration.LOGGING_ENABLED)
//    {
//      _logger.log(Level.FINE, "section " + _sectionID + " running ...");
//    }
//    _scheduler.resetRounds();
//
//    if(getActiveOperators() > 0)
//    {
//      _executed = true;
//      // kick off the scheduler
//      _scheduler.runExecutionStep();
//      _result = _scheduler.getResult();
//    }else{
//      _executed = false;
//      _result = null;
//    }
//
//    if(RuntimeProcessConfiguration.LOGGING_ENABLED)
//    {
//      _logger.log(Level.FINE, "section " + _sectionID + " returns resource!");
//    }
//
//    SectionScheduler.Activation a = prepareReturnResult(_executed);
////    System.out.println("[" + this + "] Ops acticated: " + activate.stream().map(OperatorCore::getOperatorName).collect(Collectors.toList()));
//    return a;
//  }
//
//  protected SectionScheduler.Activation prepareReturnResult(boolean executed)
//  {
//    if(RuntimeProcessConfiguration.LOGGING_ENABLED)
//    {
//      _logger.info("returning section: " + toString());
//    }
//    // System.out.println("returning section: " + toString() + " => state: " + getState());
//
//    // simple: activate all foreign operators (downstream favored)
////    Set<OperatorCore> result =
////        new HashSet<>(_scheduler.getForeignDownstreamActivations());
////    result.addAll(_scheduler.getForeignUpstreamActivations());
//    SectionScheduler.Activation a = new SectionScheduler.Activation(this);
//    a._downStreamActivations = new HashSet<>(_scheduler.getForeignDownstreamActivations());
//    a._upStreamActivations = new HashSet<>(_scheduler.getForeignUpstreamActivations());
//
//    // we need to clear them because when this section gets actually rescheduled but does not
//    // run because the request was already fulfilled then we need to make sure it does not
//    // deliver the activation requests over and over again!
//    _scheduler.clearForeignActivations();
////    if(result.isEmpty() && executed && !isSystemComponent())
////    {
////      // FIXME it would be great to enforce this assertion
////      // Assertion.invariant(!wasExecuted(), this.toString());
////    }
//    if(RuntimeProcessConfiguration.LOGGING_ENABLED)
//    {
//      _logger.info("finishing section: " + toString());
//    }
//    return a;
//  }

  public String getID()
  {
    return getSectionID().toString();
  }
  
  public boolean isSystemComponent()
  {
    return false;
  }

//  @Override
//  protected OperatorScheduler createScheduler() {
//    OperatorScheduler scheduler = new OperatorScheduler(new HashSet<>(_operators));
//    _operators.stream().forEach(o -> o.setOperatorScheduler(scheduler));
//    scheduler.setMaxQueueSize(_operators.size());
//    return scheduler;
//  }
//
//  public void setSchedulingQuanta(int quanta)
//  {
//    _scheduler.setQuanta(quanta);
//  }
//
//  public Map<String, Object> getCurrentState()
//  {
//    Map<String, Object> state = new HashMap<>();
//    state.put("scheduler-quanta", _scheduler.getQuanta());
//    return state;
//  }
//
//  public void applyState(Map<String, Object> state)
//  {
//    _scheduler.setQuanta((Integer) state.get("scheduler-quanta"));
//  }
//
//  /**
//   * Assumption: This call is only used by the section scheduler when the section is NOT
//   * running! Hence, we inherit the scheduling priority from the priority of the operators
//   * scheduled on this section.
//   *
//   * @return
//   */
//  public int getSchedulingPriority() {
//    // Assertion.invariant(getState() != SectionState.RUNNING); -> Can't enforce this here
//    // because we need the sorting also before we actually submit a section in order to
//    // understand in which order to submit them (maybe this is not actually needed). only
//    // afterwards we check whether they are already executing or not.
//    if (!isSystemComponent()) {
//      return _scheduler.getSchedulingPriority();
//    } else {
//      return _priority;
//    }
//  }
//
//  public int getActiveOperators() {
//    return _scheduler.getActiveOperators();
//  }
//
//  /**
//   * A function that allows to influence the scheduling of operators by activating this
//   * operator.
//   *
//   * @param op
//   */
//  public void activateOperator(OperatorCore op) {
//    _scheduler.setActiveOperator(op);
//  }
//
//  public String deadlockAnalysis() {
//    StringBuffer analysis = new StringBuffer();
//    analysis.append("OperatorScheduler.interrupted_operator: " + _scheduler.getInterruptedOperator() + "\n");
//    analysis.append("OperatorScheduler.last_scheduled_operator: " + _scheduler.getLastScheduledOperator() + "\n");
//    analysis.append("OperatorScheduler.rounds: " + _scheduler.getRounds() + "\n");
//    analysis.append("OperatorScheduler.activeOps: " + _scheduler.getActiveOperators() + "\n");
//    analysis.append(super.deadlockAnalysis());
//    return analysis.toString();
//  }


}
