/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.sections;

import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractNotificationBasedArcImpl.ArcEvent;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractNotificationBasedArcImpl.ArcListener;
import ohua.runtime.engine.flowgraph.elements.operator.NotificationBasedOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

import java.util.Collections;

public class ConcurrentPipelineScheduling implements ArcListener {
  private Arc _arc = null;
  private ActivationService _service = null;
  private NotificationBasedOperatorRuntime _targetRuntime;
  
  private boolean _notifyOnDataNeeded = true;
  private boolean _notifyOnDequeueNeeded = true;
  
  public ConcurrentPipelineScheduling(Arc arc, ActivationService service, NotificationBasedOperatorRuntime targetRuntime) {
    _arc = arc;
    _service = service;
    _targetRuntime = targetRuntime;
  }
  
  public void notifyOnArcEvent(ArcEvent event, Arc arc) {
    switch(event) {
      case DATA_NEEDED:
        // FIXME Is this still needed at all? Should the decision whether to schedule this upstream operator be made here or in the scheduler?!
//        if(_notifyOnDataNeeded) {
//          OperatorCore source = _arc.getSource();
//          source.activateOperator(source);
//          _service.activateSection(_arc.getSource());
//          _notifyOnDataNeeded = false;
//        }
        break;
      case DATA_AVAILABLE:
      case DEQUEUE_NEEDED:
        if(_notifyOnDequeueNeeded) {
          OperatorCore target = _arc.getTarget();
          _targetRuntime.activateOperator(_targetRuntime);
          // FIXME null passed in because I can not find the section here
          SectionScheduler.Activation a = new SectionScheduler.Activation(null);
          a._downStreamActivations = Collections.singleton(_arc.getTarget());
          a._upStreamActivations = Collections.emptySet();
          _service.activateSection(a);
          _notifyOnDequeueNeeded = false;
        }
        break;
    }
  }
  
  public void resetNotifications() {
    _notifyOnDataNeeded = true;
    _notifyOnDequeueNeeded = true;
  }
}
