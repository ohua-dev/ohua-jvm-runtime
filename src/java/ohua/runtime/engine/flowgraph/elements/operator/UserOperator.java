/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;


public abstract class UserOperator extends AbstractOperatorAlgorithm implements
                                                                    OperatorStateAccess
{
  @Override
  public final void setOperatorAlgorithmAdapter(OperatorAlgorithmAdapter adapter)
  {
    super.setOperatorAlgorithmAdapter(adapter);
  }
}
