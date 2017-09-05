/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.List;

public interface Operator
{
  // base interface for all ops
  List<OutputPort> getOutputPorts();
  
  List<InputPort> getInputPorts();
  
  OperatorID getId();
  
  String getOperatorName();
  
  // FIXME these functions really shouldn't be in here! There should be a global "admin" that
  // one could call. Then the operator should be activated through the Process interface and
  // those routines should go away!
  // these two methods will allow for instance handlers, to activate the current operator again!
  // in order to activate downstream and upstream operators even the handlers should just
  // poll/send on the ports!
//  public void activateOperator(OperatorCore owner);

}
