/*
 * Copyright (c) Sebastian Ertel 2012. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

/**
 * This thing just takes a simple reference to a JSON file which describes how the operators map
 * to the sections.
 * @author sertel
 *
 */
public class ConfigurableSectionMapper extends AbstractSectionGraphBuilder
{
  private List<List<String>> _allSections;

  public ConfigurableSectionMapper(RuntimeProcessConfiguration config)
  {
    super(config);
  }

  public void setSectionsMapping(List<List<String>> allSections){
    _allSections = allSections;
  }

  @Override
  protected SectionGraph buildSectionGraph(FlowGraph graphToConvert)
  {
    List<Section> finalSections = createSections(graphToConvert, _allSections);

    SectionGraph secGraph = new SectionGraph();
    // classify sections
    List<List<Section>> classified = classifySections(finalSections);
    secGraph.setInputSections(classified.get(0));
    secGraph.setOutputSections(classified.get(1));
    secGraph.setComputationalSections(classified.get(2));

    // assert that all operators were actually mapped to a section!
    assertAllOperatorsMapped(graphToConvert, secGraph);

    return secGraph;
  }

  private void assertAllOperatorsMapped(FlowGraph graphToConvert, SectionGraph secGraph)
  {
    for(OperatorCore op : graphToConvert.getContainedGraphNodes())
    {
      if(secGraph.findParentSection(op.getId()) == null)
      {
        throw new RuntimeException("Unmapped operator detected: " + op);
      }
    }
  }

  protected List<Section> createSections(FlowGraph graphToConvert, List<List<String>> allSections)
  {
    List<Section> finalSections = new ArrayList<>();
    for(List<String> section : allSections)
    {
      List<OperatorCore> ops = new ArrayList<>();
      for(String op : section)
      {
        ops.addAll(graphToConvert.getOperators(op));
      }
      Assertion.invariant(!ops.isEmpty(),
              "No match for section: " + Arrays.toString(section.toArray())
                      + "\nOperatorList:\n" + graphToConvert.printOperatorList());
      finalSections.add(createSection(ops));
    }
    return finalSections;
  }

}
