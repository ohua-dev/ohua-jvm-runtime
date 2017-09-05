/*
 * Copyright (c) Sebastian Ertel 2011. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

public class MultiOperatorOneSectionMapper extends AbstractSectionGraphBuilder
{
  private int _opsPerSection = 2;
  
  public MultiOperatorOneSectionMapper(RuntimeProcessConfiguration config)
  {
    super(config);
    _opsPerSection = config.getSectionSize();
  }

  @Override
  public SectionGraph buildSectionGraph(FlowGraph graphToConvert)
  {
    SectionGraph graph = new SectionGraph();
    
    List<Section> inputSections = new ArrayList<>();
    List<Section> outputSections = new ArrayList<>();
    List<OperatorCore> sourceOps = findSourceOperators(graphToConvert.getContainedGraphNodes());
    List<Section> compSections = new ArrayList<>();
    for(OperatorCore sourceOp : sourceOps)
    {
      Section sourceSection = createSingleOpSection(sourceOp);
      inputSections.add(sourceSection);
      outputSections.addAll(beginWalkBranch(compSections, sourceOp));
    }
    graph.setInputSections(inputSections);
    graph.setOutputSections(outputSections);
    graph.setComputationalSections(compSections);

    return graph;
  }

  private List<Section> beginWalkBranch(List<Section> sections, OperatorCore sourceOp)
  {
    Assertion.invariant(sourceOp.getGraphNodeOutputConnections().size() < 2, "Only pipelines supported yet!");

    Arc arc = sourceOp.getGraphNodeOutputConnections().get(0);
    OperatorCore next = arc.getTarget();

    Section newSection = new Section();
    List<OperatorCore> ops = new ArrayList<>();
    ops.add(next);

    Arc arc2 = next.getGraphNodeOutputConnections().get(0);
    return walkBranch(sections, newSection, ops, arc2.getTarget());
  }

  private List<Section> walkBranch(List<Section> sections,
                                   Section currentSection,
                                   List<OperatorCore> opsForCurrentSection,
                                   OperatorCore currentOp)
  {
    Assertion.invariant(currentOp.getGraphNodeOutputConnections().size() < 2,
                        "Only pipelines supported yet!");
    
    if(currentOp.getGraphNodeOutputConnections().size() == 0)
    {
      currentSection.setOperators(opsForCurrentSection);
      sections.add(currentSection);

      Section target = createSingleOpSection(currentOp);
      return Collections.singletonList(target);
    }
    else
    {
      Section cSection = currentSection;
      if(opsForCurrentSection.size() == _opsPerSection)
      {
        currentSection.setOperators(opsForCurrentSection);
        sections.add(currentSection);

        Section newSection = new Section();
        cSection = newSection;
        opsForCurrentSection = new ArrayList<>();
      }
      opsForCurrentSection.add(currentOp);
      
      Arc arc2 = currentOp.getGraphNodeOutputConnections().get(0);
      return walkBranch(sections, cSection, opsForCurrentSection, arc2.getTarget());
    }

  }
}
