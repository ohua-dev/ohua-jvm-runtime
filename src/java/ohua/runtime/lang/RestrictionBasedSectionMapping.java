/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import ohua.runtime.engine.ConfigurationExtension;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.sections.Section;
import ohua.runtime.engine.sections.ConfigurableSectionMapper;

/**
 * This mapper will map all unmapped operators to single sections.
 * 
 * @author sertel
 *
 */
public class RestrictionBasedSectionMapping extends ConfigurableSectionMapper {
  
  public RestrictionBasedSectionMapping(RuntimeProcessConfiguration config) {
    super(config);
    config.aquirePropertiesAccess(new ConfigurationExtension() {
      @Override
      public void setProperties(Properties properties) {
        // make sure section-config is always defined!
        if(!properties.containsKey("section-config")){
          properties.put("section-config", Collections.emptyList());
        }
      }
    });
  }
  
  protected List<Section> createSections(FlowGraph graphToConvert, List<List<String>> allSections) {
    // create the specified sections
    List<Section> specified = super.createSections(graphToConvert, allSections);
    // map every unmapped operator to its own section
    for(OperatorCore op : graphToConvert.getContainedGraphNodes()) {
      if(!isMapped(op, specified)) {
        specified.add(super.createSingleOpSection(op));
      }
    }
    return specified;
  }
  
  private boolean isMapped(OperatorCore op, List<Section> specified) {
    for(Section s : specified)
      if(s.getOperators().contains(op)) return true;
    return false;
  }
  
  /**
   * 
   * @param section
   * @param suspects
   * @return false whenever one of the suspects is outside of the section
   */
  protected boolean belongsToSection(List<OperatorCore> section, List<OperatorCore> suspects) {
    for(OperatorCore op : suspects) {
      if(!section.contains(op)) return false;
    }
    return true;
  }

}
