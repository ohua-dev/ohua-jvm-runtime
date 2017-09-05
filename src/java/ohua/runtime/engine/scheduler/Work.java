/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.flowgraph.elements.ArcID;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;


/**
 * 
 * @author sertel
 *
 */
public interface Work
{
  public OperatorID getReference();
  public ArcID getLocationReference();
  
  public int size();
  public int limit();

  public OperatorCore activate();
  public Work reportIncompleteWork();
}
