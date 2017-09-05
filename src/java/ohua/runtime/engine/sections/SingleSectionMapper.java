/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import java.util.Collections;
import java.util.List;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

public class SingleSectionMapper extends AbstractSectionGraphBuilder
{
  
  public SingleSectionMapper(RuntimeProcessConfiguration config) {
    super(config);
  }
  
  @Override protected SectionGraph buildSectionGraph(FlowGraph graphToConvert) {
    SectionGraph secGraph = new SectionGraph();
    
    FlowGraph flow = graphToConvert;
    Section sec = createSection();
    
    List<OperatorCore> containedOperators = flow.getContainedGraphNodes();
    sec.setOperators(containedOperators);

    List<Section> all = Collections.singletonList(sec);
    secGraph.setComputationalSections(all);
    secGraph.setInputSections(all);
    secGraph.setOutputSections(all);
    
    return secGraph;
  }
  
  protected Section createSection() {
    return new Section();
  }
  
}
