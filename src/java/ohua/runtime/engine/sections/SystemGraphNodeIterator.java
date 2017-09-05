/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.sections;

import ohua.runtime.engine.flowgraph.elements.GenericGraphIterator;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.sections.SectionGraph.SystemGraph;

import java.util.Comparator;
import java.util.List;

public class SystemGraphNodeIterator extends GenericGraphIterator<MetaSection>
{
  private SectionGraph _sectionGraph;

  public SystemGraphNodeIterator(SectionGraph graph)
  {
    super(graph.getSystemSections(), true);
    _sectionGraph = graph;
    super.initialize();
  }
  
  @Override
  protected void setPossibleNextOperator(MetaSection next)
  {
    if(hasMetaDownstreamOps(next))
    {
      super.setPossibleNextOperator(next);
    }
    else
    {
      _possibleNext = ((SystemGraph) _graph)._userGraphExitSection;
    }
  }

  @Override
  protected MetaSection getNextGraphNode(Arc arc) {
    return (MetaSection) _sectionGraph.findParentSection(arc.getTarget().getId());
  }

  private boolean hasMetaDownstreamOps(MetaSection next)
  {
    return next.getOutgoingArcs().stream().map(Arc::getTargetPort).allMatch(InputPort::isMetaPort);
  }

  @Override
  protected Comparator<MetaSection> getComparator() {
    return ((o1, o2) -> o1.getUniqueID().compareTo(o2.getUniqueID()));
  }

  @Override
  public MetaSection findNextOperator(MetaSection section)
  {
    if(_sectionGraph.getUserGraphExitSection() == section)
    {
      return section;
    }
    else
    {
      return super.findNextOperator(section);
    }
  }

  @Override
  protected List<Arc> getInputConnections(MetaSection section) {
    return section.getIncomingArcs();
  }

  @Override
  protected List<Arc> getOutputConnections(MetaSection section) {
    return section.getOutgoingArcs();
  }

  @Override
  protected String getID(MetaSection section) {
//    System.out.println("ID:" + section.getOperator().getOperatorName());
    return section.getID();
  }

}
