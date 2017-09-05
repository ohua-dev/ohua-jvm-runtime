/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;
import ohua.runtime.engine.sections.SectionGraph;

/**
 * This is a very simple scheduling algorithm that executes the operator with the most work to
 * do.
 * 
 * @author sertel
 * 
 */
public class BasicSchedulingAlgorithm implements SchedulingAlgorithm
{
  @Override
  public OperatorID schedule(PendingWorkOverview pendingWork, SectionGraph sectionGraph)
  {
    OperatorID sourceWork = null;
    
    int maxWork = 0;
    OperatorID maxWorkPending = null;
    
    for(OperatorID op : pendingWork.getReadyOperators())
    {
      if(sectionGraph.isSourceOperator(op))
      {
        if(sourceWork == null)
        {
          sourceWork = op;
        }
      }
      else
      {
        int pending = pendingWork.getWorkSize(op);
        if(pending > maxWork)
        {
          maxWork = pending;
          maxWorkPending = op;
        }
      }
    }
    
    return maxWorkPending != null ? maxWorkPending : sourceWork;
  }
}
