/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.scheduler;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import ohua.runtime.engine.OperatorPriorityComparator;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.operator.NotificationBasedOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

/**
 * Scheduling will be done in the following way: Schedule all operators included in the priority
 * according to their priority! We will go for a output-favored fair scheduling policy. Hence
 * all output operators will have a higher priority than the input operators, because the
 * computation is usually done in the middle of the flow. This will keep the operators from
 * accumulating state inside the flow.
 * 
 */
public class OperatorScheduler extends AbstractOperatorScheduler<NotificationBasedOperatorRuntime>
{
  public OperatorScheduler(Set<NotificationBasedOperatorRuntime> graph) {
    super(graph);
  }

  public enum SchedulerResult
  {
    QUANTA_EXHAUSTED,
    NO_READY_OPS_AVAILABLE,
  }

  private Logger _logger = Logger.getLogger(getClass().getCanonicalName());
  
  // NOTE: this parameter should really be kept very low because it helps resolving deadlocks by
  // allowing other sections to run!
  private int _quanta = 50;
  
  // should later on maybe become a PriorityBlockingQueue because multiple threads will be
  // inserting into this stuff.
  private PriorityBlockingQueue<NotificationBasedOperatorRuntime> _operators = null;

  /*
   * In the case of an interrupt we save the operator that we TRIED to interrupt because the
   * interrupt flag inside the operator needs to be reset again as well, once we are done with
   * the interrupt!
   */
  private NotificationBasedOperatorRuntime _interruptedOperator = null;
  
  // for debugging purposes
  @SuppressWarnings("unused")
  private NotificationBasedOperatorRuntime _lastScheduledOperator = null;
  
  private Set<OperatorCore> _foreignDownstreamActivations = new HashSet<>();
  private Set<OperatorCore> _foreignUpstreamActivations = new HashSet<>();
  
  private int _rounds = 0;

  private int _round = 0;
  private SchedulerResult _result = null;

  public void setMaxQueueSize(int maxQueueSize)
  {
    _operators =
        new PriorityBlockingQueue<>(maxQueueSize, new OperatorPriorityComparator());
  }
  
  public void setQuanta(int quanta)
  {
    _quanta = quanta;
  }
  
  public int getQuanta()
  {
    return _quanta;
  }

  public SchedulerResult getResult(){
    return _result;
  }

  protected void beforeExecutionStep() {
    _round = 0;
    _rounds = _round;
    _result = SchedulerResult.NO_READY_OPS_AVAILABLE;

  }

  @Override
  protected Optional<NotificationBasedOperatorRuntime> schedule() {
    if(!_operators.isEmpty()) {
      NotificationBasedOperatorRuntime op = _operators.poll();
      // FIXME this is not supposed to happen during INIT, otherwise we deadlock!
      if (_round > _quanta) {
        _rounds = _round;
        if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
          _logger.log(Level.ALL, ": Quanta exhausted!");
        }

        // put the currently dequeued operator back into the queue because it has not been
        // worked yet!
        setActiveOperator(op);

        // make sure we get activated again
        _foreignDownstreamActivations.add(op.getOp());
        _result = SchedulerResult.QUANTA_EXHAUSTED;
        return Optional.empty();
      }

      // if we reach here and an interrupt is received, the operator will return because
      // _currentOperator is set

      op.resetQuanta();
      _lastScheduledOperator = op;
      return Optional.of(op);
    }else{
      return Optional.empty();
    }
  }

  @Override
  protected void handleDoneExecution(NotificationBasedOperatorRuntime op) {
    _round++;
  }

  protected void afterExecutionStep() {
    _rounds = _round;

    if(_round < 1)
    {
      _logger.warning("Redundant schedule of section detected!");
    }
  }

  /**
   * Activates the operator for scheduling and pushes it back into the queue of the scheduler
   * (if not already present).<br/>
   * Note that we don NOT lose notifications here because we do not have deactivated operators
   * inside the scheduler queue. Hence this operator is either:
   * <ol>
   * <li>deactive (not in scheduler queue and not active)
   * <li>activated (in the scheduler queue and therefore active or soon to be activated)
   * <li>active (not in the scheduler => running)
   * </ol>
   * @param op Operator to be activated.
   */
  public void setActiveOperator(NotificationBasedOperatorRuntime op)
  {
    op.activate();
    
    if(!_operators.contains(op))
    {
      _operators.add(op);
    }
  }

  public NotificationBasedOperatorRuntime getLastScheduledOperator()
  {
    return _lastScheduledOperator;
  }

  public NotificationBasedOperatorRuntime getInterruptedOperator()
  {
    return _interruptedOperator;
  }
  
  public int getActiveOperators()
  {
    return _operators.size();
  }

  public int getSchedulingPriority()
  {
    // Assertion.invariant(!_operators.isPresent()); --> this is tricky to enforce because
    // apparently it can happen that we have redundant executions of sections inside the system.
    // this can for instance happen when a section received a notification while it is running.
    // the section is then scheduled again although it already finished processing.
    NotificationBasedOperatorRuntime nextOp = _operators.peek(); // concurrency-safe!
    if(nextOp == null)
    {
      // the nice thing about it is that whenever this things gets reordered the priority can
      // increase because another section just activated one of the operators.
      return 0;
    }
    else
    {
      return nextOp.getGraphPriority();
    }
  }
  
  // TODO This should not be an upcall but the activation of other operators should really
  // happen here!
  public void notifyForeignOperatorActivation(OperatorCore op, boolean downstream)
  {
    if(downstream)
    {
      _foreignDownstreamActivations.add(op);
    }
    else
    {
      _foreignUpstreamActivations.add(op);
    }
  }
  
  public Set<OperatorCore> getForeignDownstreamActivations()
  {
    return _foreignDownstreamActivations;
  }
  
  public Set<OperatorCore> getForeignUpstreamActivations()
  {
    return _foreignUpstreamActivations;
  }
  
  public void clearForeignActivations()
  {
    _foreignDownstreamActivations.clear();
    _foreignUpstreamActivations.clear();
  }

  public int getRounds()
  {
    return _rounds;
  }
  
  public void resetRounds()
  {
    _rounds = 0;
  }

  public String deadlockAnalysis() {
    StringBuffer analysis = new StringBuffer();
    _graph.stream().map(NotificationBasedOperatorRuntime::deadlockAnalysis).forEach(analysis::append);
    return analysis.toString();
  }

}
