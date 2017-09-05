/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

import java.util.Set;

import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;

public interface IOutputPortEventHandler
{
  public int getPriority(OutputPortEvents event);
  public Set<OutputPortEvents> getOutputPortEventInterest();
  public void notifyOutputEvent(OutputPort port, OutputPortEvents event);
}
