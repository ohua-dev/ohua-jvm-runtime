/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators.system;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.SystemOperator;

/**
 * 
 * @author sertel
 * 
 */
public class UserGraphEntranceOperator extends SystemOperator implements OperatorStateAccess
{

  @Override
  public void cleanup()
  {
    // nothing
  }
  
  @Override
  public void prepare()
  {
    // nothing
  }
  
  @Override
  public void runProcessRoutine()
  {
    // nothing to do here. this is a pure meta data operator and a funnel. everything is
    // processes just as in all the other operators by the marker handlers.
  }
  
  /**
   * We implement this interface because the entrance needs to route the markers into the flow.
   */
  public Object getState()
  {
    Assertion.invariant(false);
    return null;
  }
  
  public void setState(Object checkpoint)
  {
    Assertion.invariant(false);
  }

  @Override
  protected boolean isSourceOperatorWasLastPacket()
  {
    // this can never happen because this operator always has at least one input port now
    Assertion.invariant(false);
    return false;
  }

}
