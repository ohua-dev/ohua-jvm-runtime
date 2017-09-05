/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.sections.Section;
import ohua.runtime.engine.sections.SectionGraph;
import ohua.runtime.engine.sections.AbstractSection;
import ohua.runtime.engine.sections.AbstractSection.SectionsComparator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

public class GraphVisualizer {
  public static String PRINT_SECTION_GRAPH = null;
  public static String PRINT_FLOW_GRAPH = null;

  private static PrintStream _output;

  public static void printFlowGraph(FlowGraph graph) {
    if (PRINT_FLOW_GRAPH == null) {
      return;
    }

    try {
      _output = new PrintStream(new FileOutputStream(PRINT_FLOW_GRAPH + ".dot"));
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException("unhandled exception", e);
    }

    _output.println("digraph FLOW {");
    _output.println("labelloc=top;");
    _output.println("rankdir=LR;");

    printOperators("", graph.getContainedGraphNodes());

    printArcs("", graph.getContainedArcsFromOps());

    _output.println("}");
    _output.flush();
    _output.close();
  }

  private static void printArcs(String pathPrefix, List<Arc> containedArcs) {
    for (Arc arc : containedArcs) {
      if (!arc.getSourcePort().isMetaPort() && !arc.getTargetPort().isMetaPort()) {
        _output.println(pathPrefix + arc.getSource().getId().getIDInt() + " -> " + pathPrefix
                + arc.getTarget().getId().getIDInt() + " [label=\"("
                + arc.getSourcePort().getPortName() + ", "
                + arc.getTargetPort().getPortName() + ",\n"
                + arc.getType().name() + ")\"];");
      }
    }
  }

  private static void printOperators(String subGraphPrefix,
                                     List<OperatorCore> containedOperators) {
    for (OperatorCore op : containedOperators) {
      if (!op.isSystemComponent())
        _output.println(subGraphPrefix + op.getId().getIDInt()
                + " [shape=rectangle, fontsize=6, label=\""
                + "name: " + op.getOperatorName()
                + " id: " + op.getId()
//                + " priority: " + op.getGraphPriority()
                + "\"];");
    }

  }

  @SuppressWarnings("unchecked")
  public static void printSectionGraph(SectionGraph graph) {
    if (PRINT_SECTION_GRAPH == null) {
      return;
    }

    try {
      _output = new PrintStream(new FileOutputStream(PRINT_SECTION_GRAPH + ".dot"));
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException("unhandled exception", e);
    }

    _output.println("digraph FLOW {");
    _output.println("labelloc=top;");
    _output.println("rankdir=LR;");

    List<Section> sections = new ArrayList<Section>(graph.getAllSections());
    List<Arc> arcs = new ArrayList<Arc>();
    Collections.sort(sections, new SectionsComparator());
    for (AbstractSection section : sections)
      arcs.addAll(printSection(section));

    Collections.sort(arcs, (o1,o2) -> {
        int i = o1.getSource().getId().compareTo(o2.getSource().getId());
        return i != 0 ? i
                : o1.getTarget().getId().compareTo(o2.getTarget().getId());
    });
    printArcs("", arcs);

    _output.println("}");
    _output.flush();
    _output.close();
  }

  private static List<Arc> printSection(AbstractSection section) {
    // do the subgraph set up
    _output.println("subgraph \"cluster_" + section.getSectionID() + "\" {");
    _output.println("label=\"Section " + section.getSectionID() + "\";");

    // print the ops of the section
    String pathPrefix = "";
    List<OperatorCore> operators = new ArrayList<>(section.getOperators());
    Collections.sort(operators, (o1, o2) -> o1.getId().compareTo(o2.getId()));
    printOperators(pathPrefix, operators);

    // print all (section local) arcs
    List<Arc> sectionLocalArcs = new LinkedList<>();
    List<Arc> interSectionArcs = new LinkedList<>();
    for (OperatorCore compOrLastOp : operators) {
      for (InputPort inPort : compOrLastOp.getInputPorts()) {
        if (section.getOperators().contains(inPort.getIncomingArc().getSource())) {
          sectionLocalArcs.add(inPort.getIncomingArc());
        } else {
          interSectionArcs.add(inPort.getIncomingArc());
        }
      }
    }

    printArcs(pathPrefix, sectionLocalArcs);
    // wrap up this section graph
    _output.println("}");
    return interSectionArcs;
  }
}
