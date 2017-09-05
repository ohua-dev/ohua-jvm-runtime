/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.abstraction;

import java.util.List;

import ohua.runtime.engine.flowgraph.elements.AbstractUniqueID;

@SuppressWarnings("rawtypes")
public interface GraphNode
{
  List<? extends GraphArc> getGraphNodeInputConnections();
  List<? extends GraphArc> getGraphNodeOutputConnections();

  int getNumGraphNodeInputs();
  int getNumGraphNodeOutputs();
  
//  public List<GraphArc> getAllOutputGraphConnections();
  List<? extends GraphNode> getAllPreceedingGraphNodes();
  List<? extends GraphNode> getAllSucceedingGraphNodes();
  
  String getID();
  
  AbstractUniqueID getUniqueID();

  boolean isSystemComponent();
}
