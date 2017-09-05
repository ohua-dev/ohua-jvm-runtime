/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.sections;

import ohua.runtime.engine.flowgraph.elements.GenericGraphIterator;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;

import java.util.Comparator;
import java.util.List;

/**
 * Created by sertel on 1/20/17.
 */
public class SectionGraphIterator extends GenericGraphIterator<Section>{

  private SectionGraph _sectionGraph;

  public SectionGraphIterator(SectionGraph sectionGraph) {
    super(sectionGraph.getContainedGraphNodes());
    _sectionGraph = sectionGraph;
    super.initialize();
  }

  @Override
  protected Comparator<Section> getComparator() {
    return ((o1, o2) -> o1.getUniqueID().compareTo(o2.getUniqueID()));
  }

  @Override
  protected List<Arc> getInputConnections(Section section) {
    return section.getIncomingArcs();
  }

  @Override
  protected List<Arc> getOutputConnections(Section section) {
    return section.getOutgoingArcs();
  }

  @Override
  protected String getID(Section section) {
    return section.getID();
  }

  @Override
  protected Section getNextGraphNode(Arc arc) {
    return _sectionGraph.findParentSection(arc.getTarget().getId());
  }
}
