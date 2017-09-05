/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

public class OneOpOneSectionGraphBuilder extends AbstractSectionGraphBuilder
{
  protected Map<OperatorID, Section> _sectionMap = new HashMap<OperatorID, Section>();
  
  public OneOpOneSectionGraphBuilder(RuntimeProcessConfiguration config)
  {
    super(config);
  }
  
  @Override
  public SectionGraph buildSectionGraph(FlowGraph graphToConvert)
  {
    _sectionMap.clear();
    
    SectionGraph sectionGraph = new SectionGraph();
    
    List<OperatorCore> sourceOps =
        findSourceOperators(graphToConvert.getContainedGraphNodes());
    
    List<Section> inputSections = new ArrayList<Section>();
    for(OperatorCore op : sourceOps)
    {
      walkBranch(op);
      Section sourceSection = _sectionMap.get(op.getId());
      assert sourceSection != null;
      inputSections.add(sourceSection);
    }
    
    sectionGraph.setInputSections(inputSections);
    
    List<Section> computationalSections = findComputationalSections();
    sectionGraph.setComputationalSections(computationalSections);
    
    List<Section> outputSections = findOutputSections();
    sectionGraph.setOutputSections(outputSections);
    
//    List<SectionArc> interSectionArcs = findInterSectionArcs();
//    sectionGraph.setInterSectionArcs(interSectionArcs);
    
    return sectionGraph;
  }
    
//  private List<SectionArc> findInterSectionArcs()
//  {
//    List<SectionArc> sectionArcs = new ArrayList<SectionArc>();
//
//    HashSet<Section> sections = new HashSet<Section>(_sectionMap.values());
////    for(Section section : sections)
////    {
////      sectionArcs.addAll(section.getIncomingArcs());
////    }
//
//    return sectionArcs;
//  }
  
  private List<Section> findOutputSections()
  {
    List<Section> outputSections = new ArrayList<Section>();
    
    for(Section section : _sectionMap.values())
    {
      if(section.getNumGraphNodeInputs() > 0 && section.getNumGraphNodeOutputs() < 1)
      {
        outputSections.add(section);
      }
    }
    
    return outputSections;
  }
  
  private List<Section> findComputationalSections()
  {
    List<Section> compSections = new ArrayList<Section>();
    
    for(Section section : _sectionMap.values())
    {
      if(section.getNumGraphNodeInputs() > 0 && section.getNumGraphNodeOutputs() > 0)
      {
        compSections.add(section);
      }
    }
    
    return compSections;
  }
    
  protected void walkBranch(OperatorCore source)
  {
    createSingleOpSection(source);
    
    for(OperatorCore nextOp : source.getAllSucceedingGraphNodes())
    {
      if(_sectionMap.containsKey(nextOp.getId()))
      {
        continue;
      }
      
      walkBranch(nextOp);
    }
    
//    createSectionArcs(source);
  }

  // TODO this belongs now also into the abstract class!
//  private void createSectionArcs(OperatorCore source)
//  {
//    Section section = _sectionMap.get(source.getId());
//    List<SectionArc> outgoingArcs = new ArrayList<>();
//    for(Arc outArc : source.getGraphNodeOutputConnections())
//    {
//      if(!section.getOperators().contains(outArc.getTarget())) {
//        Section targetSection = _sectionMap.get(outArc.getTarget().getId());
//
//        SectionArc sectionArc = new SectionArc();
//        sectionArc.setSourceSection(section);
//        sectionArc.setTargetSection(targetSection);
//        sectionArc.setMappedArc(outArc);
//
////        targetSection.getIncomingArcs().add(sectionArc);
//        outgoingArcs.add(sectionArc);
//      }
//    }
//
////    section.setOutgoingArcs(outgoingArcs);
//  }
  
//  protected List<Arc> getSectionOutputConnections(Section section, OperatorCore source)
//  {
//    return source.getGraphNodeOutputConnections();
//  }

  @Override
  public Section createSingleOpSection(OperatorCore source)
  {
    Section newSection = super.createSingleOpSection(source);
    _sectionMap.put(source.getId(), newSection);
    return newSection;
  }
  
}
