/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph;

import java.util.LinkedList;

import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.sections.SectionGraph;

public interface GraphAnalysisAlgorithm
{
  /**
   * Setup and register marker visitors here.
   * @param sectionGraph
   */
  public void prepareGraphAnalysisInfrastructure(SectionGraph sectionGraph);
  
  /**
   * Register handlers and do the general setup of the algorithm here.
   * @param graph
   */
  public void prepareGraphAnalysisAlgorithm(SectionGraph graph);
  
  /**
   * Get the markers to be send through the graph. The order in the result list will be the
   * order in which they are being pushed onto the stream.
   * @return
   */
  public LinkedList<IMetaDataPacket> getAnalysisMarkers();

}
