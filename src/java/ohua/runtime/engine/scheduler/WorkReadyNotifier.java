/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.HashSet;
import java.util.Set;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.IOutputPortEventHandler;
import ohua.runtime.engine.points.OutputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractNotificationBasedArcImpl;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractNotificationBasedArcImpl.ArcEvent;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractNotificationBasedArcImpl.ArcListener;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;

@Deprecated
public class WorkReadyNotifier implements ArcListener, IOutputPortEventHandler
{
  private TaskScheduler _scheduler = null;
  
  protected WorkReadyNotifier(TaskScheduler scheduler) {
    _scheduler = scheduler;
  }
  
  /**
   * Arguments are:<br>
   * <ul>
   * <li>Arc
   * <li>WorkChunk
   * </ul>
   */
  @Override public void notifyOnArcEvent(ArcEvent event, Arc arc) {
    Assertion.invariant(event == AbstractNotificationBasedArcImpl.ArcEvent.DEQUEUE_NEEDED);
    reportWork(arc);
  }
  
  private void reportWork(Arc arc) {
    TaskBasedAsynchronousArc arcImpl = (TaskBasedAsynchronousArc) arc.getImpl();
    WorkChunk chunk = arcImpl.retrieveWork();
    Assertion.invariant(chunk.size() > 0);
    OperatorID id = arc.getTarget().getId();
    Work work = new SourceWork(arc, chunk);
//    _scheduler.notifyWorkReady(id, work);
  }
  
  @Override public Set<OutputPortEvents> getOutputPortEventInterest() {
    Set<OutputPortEvents> events = new HashSet<OutputPortEvents>();
    events.add(OutputPortEvents.OUTPUT_PORT_FINISHED);
    events.add(OutputPortEvents.OUTPUT_PORT_CLOSED);
    return events;
  }
  
  // The enqueuing of the EOS also only happens in an handler! therefore, we have to make
  // sure that we are being called after this handler, otherwise we will not find the EOS as
  // work that still needs to processed!
  public int getPriority(OutputPortEvents event) {
    return -10;
  }
  
  @Override public void notifyOutputEvent(OutputPort port, OutputPortEvents event) {
    Assertion.invariant(event == OutputPortEvents.OUTPUT_PORT_FINISHED
                        || event == OutputPortEvents.OUTPUT_PORT_CLOSED);
    for(Arc outArc : port.getOutgoingArcs()) {
      if(((TaskBasedAsynchronousArc) outArc.getImpl()).hasPendingWork()) {
        reportWork(outArc);
      }
    }
  }
  
}
