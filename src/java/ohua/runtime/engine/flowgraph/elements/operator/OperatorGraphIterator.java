/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.flowgraph.elements.GenericGraphIterator;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by sertel on 1/20/17.
 */
public class OperatorGraphIterator extends GenericGraphIterator<OperatorCore> {

  public OperatorGraphIterator(List<OperatorCore> graph) {
    super(graph);
    super.initialize();
  }

  @Override
  protected Comparator<OperatorCore> getComparator() {
    return ((o1, o2) -> o1.getUniqueID().compareTo(o2.getUniqueID()));
  }

  @Override
  protected List<Arc> getInputConnections(OperatorCore op) {
    return op.getInputPorts().stream().map(InputPort::getIncomingArc).collect(Collectors.toList());
  }

  @Override
  protected List<Arc> getOutputConnections(OperatorCore op) {
    return op.getOutputPorts().stream().flatMap(o -> o.getOutgoingArcs().stream()).collect(Collectors.toList());
  }

  @Override
  protected String getID(OperatorCore op) {
    return op.getID();
  }

  @Override
  protected OperatorCore getNextGraphNode(Arc arc) {
//    System.out.println("Next node: " + arc.getTarget().getOperatorName());
    return arc.getTarget();
  }
}
