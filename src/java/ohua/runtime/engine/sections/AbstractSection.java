/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.AbstractUniqueID;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public abstract class AbstractSection/*<T extends AbstractOperatorScheduler> implements Callable<SectionScheduler.Activation> */ {
  protected SectionID _sectionID = SectionIDGenerator.generateNewSectionID();
  protected Logger _logger = Logger.getLogger(getClass().getCanonicalName());
//  protected T _scheduler = null;

  // FIXME should be a set
  protected List<OperatorCore> _operators = null;
  protected int _priority = 100;

//  private Periodic _periodicOp = null;
//  private long _totalComputationTime = 0;
//  private long _cycleRuntime = 0;

  public abstract String getID();

  public abstract boolean isSystemComponent();

  public AbstractUniqueID getUniqueID() {
    return _sectionID;
  }

  public void assignGraphPriority(int priority) {
    _priority = priority;
  }

  public final int getGraphPriority(){
    return _priority;
  }

  public List<OperatorCore> getOperators() {
    return Collections.unmodifiableList(_operators);
  }

  public void setOperators(List<OperatorCore> operators) {
    if(operators.isEmpty()) {
      throw new IllegalArgumentException("List of ops set for section must not be empty!");
    }else{
      _operators = operators;
//      _scheduler = createScheduler();
    }
  }

//  abstract protected T createScheduler();

  public SectionID getSectionID() {
    return _sectionID;
  }

  public int getNumGraphNodeInputs() {
    return getIncomingArcs().size();
  }

  public int getNumGraphNodeOutputs() {
    return getOutgoingArcs().size();
  }

  public List<Arc> getIncomingArcs() {
    return _operators.stream()
            .flatMap(o -> o.getInputPorts().stream())
            .map(InputPort::getIncomingArc)
            .filter(a -> !_operators.contains(a.getSource()))
            .collect(Collectors.toList());
  }

  public List<Arc> getOutgoingArcs() {
    return _operators.stream()
            .flatMap(o -> o.getOutputPorts().stream())
            .flatMap(o -> o.getOutgoingArcs().stream())
            .filter(a -> !_operators.contains(a.getTarget()))
            .collect(Collectors.toList());
  }

  public void printSectionInfo() {
    if (RuntimeProcessConfiguration.LOGGING_ENABLED)
      printSectionInfo(_logger);
  }

  public void printSectionInfo(Logger logger) {
    logger.info("Section info:");
    logger.info("section id = " + _sectionID);
    logger.info("operators included in this section = ");
    for (OperatorCore op : _operators) {
      logger.info(op.getOperatorName() + " with ID = " + op.getId());
    }
  }

  @Override
  public String toString() {
    return "Section(" + getID() + ", " + _operators.get(0).getOperatorName() + ", " + _priority + ")";
  }

//  public boolean isPeriodic() {
//    return _periodicOp != null;
//  }

//  protected void initializeSection() {
//    for (OperatorCore op : getOperators()) {
//      if (op.getOperatorAlgorithm() instanceof Periodic) {
//        // currently only one periodic op per section is supported
//        _periodicOp = (Periodic) op.getOperatorAlgorithm();
//        break;
//      }
//    }
//  }

//  // FIXME should go into a different type of section!
//  public long getPeriod() {
//    if (_periodicOp == null) {
//      return 0;
//    }
//
//    long period = _periodicOp.getTimePeriod();
//    long lastPT = TimeUnit.MILLISECONDS.convert(_cycleRuntime, getPeriodTimeUnit());
//
//    if (period - lastPT < 0) {
//      return 0;
//    } else {
//      return period - lastPT;
//    }
//  }
//
//  public TimeUnit getPeriodTimeUnit() {
//    return _periodicOp.getTimeUnit();
//  }

//  public final SectionScheduler.Activation call() {
//    long start = System.currentTimeMillis();
//    SectionScheduler.Activation toBeActivated = runSectionCycle();
//    _cycleRuntime = System.currentTimeMillis() - start;
//    _totalComputationTime += _cycleRuntime;
//
//    return toBeActivated;
//  }
//
//  abstract protected SectionScheduler.Activation runSectionCycle();

//  public long getTotalComputationTime() {
//    return _totalComputationTime;
//  }
//
//  public void startNewSystemPhase() {
//    _totalComputationTime = 0;
//  }

//  public boolean hasIOOperator() {
//    // TBD Not sure whether this part should be dealing with the operator cores or rather with
//    // the
//    // operator interfaces only.
//    for (OperatorCore operator : getOperators()) {
//      if (operator.getOperatorAlgorithm() instanceof IOOperator) {
//        return true;
//      }
//    }
//
//    return false;
//  }

//  abstract public Map<String, Object> getCurrentState();
//
//  abstract public void applyState(Map<String, Object> state);

  public boolean isSourceSection() {
    return !isSystemComponent() && getOperators().stream().filter(op -> !op.isSystemInputOperator() && op.isSystemOutputOperator()).count() == 1;
  }

  public boolean isTargetSection() {
    return !isSystemComponent() && getOperators().stream().filter(op -> !op.isSystemOutputOperator() && op.isSystemInputOperator()).count() == 1;
  }

//  public String deadlockAnalysis() {
//    StringBuffer analysis = new StringBuffer();
//    _operators.stream().map(OperatorCore::deadlockAnalysis).forEach(analysis::append);
//    return analysis.toString();
//  }

  public static class SectionID extends AbstractUniqueID {
    public SectionID(int id) {
      super(id);
    }
  }

  public static class SectionIDGenerator {
    private static int _counter = 0;

    public static SectionID generateNewSectionID() {
      return new SectionID(_counter++);
    }

    public static void restCounter() {
      _counter = 0;
    }
  }

  @SuppressWarnings("rawtypes")
  public static final class SectionsComparator implements Comparator {
    public int compare(Object o1, Object o2) {
      return ((AbstractSection) o1).getID().compareTo(((AbstractSection) o2).getID());
    }
  }
}
