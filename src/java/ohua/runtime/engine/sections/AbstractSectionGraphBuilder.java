/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

public abstract class AbstractSectionGraphBuilder {
//  protected Map<OperatorID, OperatorCore> _inputOps = new HashMap<>();
//  protected Map<OperatorID, OperatorCore> _outputOps = new HashMap<>();
  
  public AbstractSectionGraphBuilder(RuntimeProcessConfiguration config) {
    // nothing yet
  }
  
  public final SectionGraph build(FlowGraph graphToBeConverted) {
//    retrieveIOsectionCandidates(graphToBeConverted);
    SectionGraph graph = buildSectionGraph(graphToBeConverted);
//    graph.setIoSections(findIOSections(graph));
    graphBuildDone(graph);
    return graph;
  }
  
  protected void graphBuildDone(SectionGraph graph) {
//    GraphVisualizer.printSectionGraph(graph);
  }
  
  protected abstract SectionGraph buildSectionGraph(FlowGraph graphToConvert);
  
//  private void retrieveIOsectionCandidates(FlowGraph graphToBeConverted) {
//    for(OperatorCore op : graphToBeConverted.getContainedGraphNodes()) {
//      if(op.getOperatorAlgorithm() instanceof InputIOOperator) {
//        _inputOps.put(op.getId(), op);
//        continue;
//      }
//
//      if(op.getOperatorAlgorithm() instanceof OutputIOOperator) {
//        _outputOps.put(op.getId(), op);
//        continue;
//      }
//    }
//  }
  
  protected List<OperatorCore> findSourceOperators(List<OperatorCore> containedOperators) {
    List<OperatorCore> sourceOps = new ArrayList<OperatorCore>();
    for(OperatorCore containedOp : containedOperators) {
      if(containedOp.getNumGraphNodeInputs() < 1) {
        sourceOps.add(containedOp);
      }
    }
    
    return sourceOps;
  }
  
  protected List<OperatorCore> findTargetOperators(List<OperatorCore> containedOperators) {
    List<OperatorCore> targetOps = new ArrayList<>();
    for(OperatorCore containedOp : containedOperators) {
      if(containedOp.getNumGraphNodeOutputs() < 1) {
        targetOps.add(containedOp);
      }
    }
    
    return targetOps;
  }
  
  public Section createSingleOpSection(OperatorCore source) {
    Section newSection = new Section();
    newSection.setOperators(new ArrayList<>(Collections.singletonList(source)));
    return newSection;
  }

//  private List<Section> findIOSections(SectionGraph graph) {
//    Set<Section> ioSections = new HashSet<>();
//    ioSections.addAll(findIOSections(graph.getAllSections()));
//    ioSections.addAll(findIOSections(graph.getInputSections()));
//    ioSections.addAll(findIOSections(graph.getOutputSections()));
//    return new ArrayList<>(ioSections);
//  }
  
//  private List<Section> findIOSections(Collection<Section> sections) {
//    List<Section> ioSections = new ArrayList<>();
//
//    for(Section section : sections) {
//      for(OperatorCore op : section.getOperators()) {
//        if (op.getOperatorAlgorithm() instanceof OutputIOOperator
//                || op.getOperatorAlgorithm() instanceof InputIOOperator) {
//          ioSections.add(section);
//        }
//      }
//    }
//
//    return ioSections;
//  }
  
  protected final Section createSection(List<OperatorCore> ops) {
    Section newSection = new Section();
    newSection.setOperators(ops);
    return newSection;
  }

  /**
   * Returns a list of three lists: input sections, output sections and computational sections.
   * @param finalSections
   * @return
   */
  protected final List<List<Section>> classifySections(List<Section> finalSections) {
    List<Section> inputSections = new ArrayList<>();
    List<Section> outputSections = new ArrayList<>();
    List<Section> computationalSections = new ArrayList<>();
    
    for(Section section : finalSections) {
      if(section.isSourceSection()) {
        inputSections.add(section);
      } else if(section.isTargetSection()) {
        outputSections.add(section);
      } else {
        computationalSections.add(section);
      }
    }
    
    List<List<Section>> result = new ArrayList<List<Section>>();
    Collections.addAll(result, inputSections, outputSections, computationalSections);
    return result;
  }
  
}
