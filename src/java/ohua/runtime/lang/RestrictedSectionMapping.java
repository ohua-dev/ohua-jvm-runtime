/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;
import ohua.runtime.engine.sections.AbstractSectionGraphBuilder;
import ohua.runtime.engine.sections.Section;
import ohua.runtime.engine.sections.SectionGraph;
import ohua.runtime.engine.sections.AbstractSection.SectionID;

public class RestrictedSectionMapping extends AbstractSectionGraphBuilder {
  
  private AbstractSectionGraphBuilder _specifiedBuilder = null;
  // TODO whenever we start to introduce dynamic section configuration at runtime (which might
  // not be long down the road anymore), we have to make sure that this algorithm also respects
  // these restrictions!
  private List<OperatorID[]> _restrictions = null;
  
  public RestrictedSectionMapping(RuntimeProcessConfiguration config,
                                  AbstractSectionGraphBuilder specifiedBuilder,
                                  List<OperatorID[]> restrictions)
  {
    super(config);
    _specifiedBuilder = specifiedBuilder;
    _restrictions = restrictions;
  }
  
  @Override
  protected SectionGraph buildSectionGraph(FlowGraph graphToConvert) {
    SectionGraph secGraph = _specifiedBuilder.build(graphToConvert);
    // check for the restrictions and enforce them if necessary
    for(OperatorID[] restriction : _restrictions) {
      enforceRestriction(secGraph, restriction);
    }
    return secGraph;
  }
  
  /**
   * The simple strategy for now, in case not all operators are on the same section, is to
   * migrate them all to a new section. Don't forget to handle orphaned sections!
   * 
   * @param secGraph
   * @param restriction
   */
  private void enforceRestriction(SectionGraph secGraph, OperatorID[] restriction) {
    HashSet<SectionID> sections = new HashSet<SectionID>();
    for(OperatorID victim : restriction)
      sections.add(secGraph.findParentSection(victim).getSectionID());
    
    // enforce
    if(sections.size() > 1) {
      List<OperatorCore> victims = new ArrayList<>();
      // unmap
      for(OperatorID victim : restriction) {
        OperatorCore op = secGraph.findOperator(victim);
        victims.add(op);
        secGraph.remove(op); // evicts the op and the section if necessary
      }
      
      // map to new section and register at the section graph
      Section enforcedRestriction = super.createSection(victims);
      List<List<Section>> classification = super.classifySections(Collections.singletonList(enforcedRestriction));
      classification.get(0).addAll(secGraph.getInputSections());
      classification.get(1).addAll(secGraph.getOutputSections());
      classification.get(2).addAll(secGraph.getComputationalSections());
      secGraph.setInputSections(classification.get(0));
      secGraph.setOutputSections(classification.get(1));
      secGraph.setComputationalSections(classification.get(2));
      secGraph.updateOperatorRegistry(Collections.singletonList(enforcedRestriction));
    }
  }
  
}
