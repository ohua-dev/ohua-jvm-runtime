/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

/**
 * The priority of the sections in the user graph increases towards the targets in order to
 * implement output-favored scheduling that allows for low latency. On the other hand, system
 * sections receive priorities higher than any of the user graph sections. This allows to
 * schedule system sections online and makes sure they are being executed close to their
 * submission.
 * @author sertel
 * 
 */
public class SectionPriorityAdvisor
{
  public void assignPriorities(SectionGraph graph)
  {
    int priority = Thread.MIN_PRIORITY;
    
    // give all input sections the same priority
    for(AbstractSection inputSection : graph.getInputSections())
    {
      inputSection.assignGraphPriority(priority);
    }
    
    priority = graph.getAllOperators().size() + 1;
    // handle the system sections
    SystemGraphNodeIterator iter = graph.getSystemGraphIterator();
    while(iter.hasNext())
    {
      MetaSection section = iter.next();
      section.assignGraphPriority(priority++);
    }
    
    graph.getUserGraphEntranceSection().assignGraphPriority(priority++);
    graph.getUserGraphExitSection().assignGraphPriority(0);
  }
}
