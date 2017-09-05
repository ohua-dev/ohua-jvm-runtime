/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import java.util.Collections;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

// FIXME should this inherit from another type of section? what scheduler does it require?!
public class MetaSection extends Section
{
  @Override
  public boolean isSystemComponent()
  {
    return true;
  }

  public void createSingleOperatorSection(OperatorCore metaOp)
  {
    setOperators(Collections.singletonList(metaOp));
  }
  
  public OperatorCore getOperator()
  {
    Assertion.invariant(getOperators().size() == 1);
    return getOperators().get(0);
  }
}
