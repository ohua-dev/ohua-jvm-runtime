/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import java.util.List;

import junit.framework.Assert;

import ohua.runtime.test.AbstractFlowTestCase;
import org.junit.Test;

import ohua.runtime.engine.AbstractProcessManager;

public class testSectionGraphBuilder extends AbstractFlowTestCase
{
      
  @Test
  public void testOneOpOneSection() throws Throwable
  { 
    AbstractProcessManager manager =
        loadProcess(getTestMethodInputDirectory() + "transactions-coloring-flow.xml");
    OneOpOneSectionGraphBuilder builder = new OneOpOneSectionGraphBuilder(null);
    SectionGraph graph = builder.build(manager.getProcess().getGraph());
    List<Section> sections = graph.getContainedGraphNodes();
    Assert.assertEquals(sections.size(), manager.getProcess().getGraph().getContainedGraphNodes().size());
  }
  
}
