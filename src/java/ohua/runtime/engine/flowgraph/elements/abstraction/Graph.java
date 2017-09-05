/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.abstraction;

import java.util.List;


public interface Graph<T extends GraphNode>
{
  public List<T> getContainedGraphNodes();
}
