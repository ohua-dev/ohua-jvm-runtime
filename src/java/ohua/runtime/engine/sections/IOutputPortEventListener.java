/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;

public interface IOutputPortEventListener
{
  // called from the port. this is supposed to set the section into a state where it is capable
  // of receiving notifications from downstream.
  //TODO: remove the argument!
  public void notifyAtLeastOneArcBlocking(Arc op);
  
  // called when there can be data inserted again into this port
  public void notifyPortCanConsumeData(OutputPort port);
  
  // the state of a port has changed and we might adjust the priority of the section or even
  // block the thread
  @Deprecated // should use the now operator notification interface!
  public void notifyOnPortStateChange();
}
