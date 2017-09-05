/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;
import ohua.runtime.engine.sections.SectionGraph;

public interface SchedulingAlgorithm
{
  // TODO maybe it is better to allow for a list of work to be returned
  // TODO we also should only provide a restricted view on the section graph!
  public OperatorID schedule(PendingWorkOverview pendingWork, SectionGraph sectionGraph);
}
