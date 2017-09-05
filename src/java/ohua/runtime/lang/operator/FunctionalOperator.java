/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

/**
 * Created by sebastianertel on 3/23/16.
 */
public final class FunctionalOperator extends AbstractFunctionalOperator {

  protected AbstractSchemaMatcher getSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
    if (super.getInputPorts().isEmpty())
      return new FunctionalSchemaMatching.FunctionalSourceSchemaMatcher(functionalOperator, ohuaOperator);
    // put this check before the target check because targets always work and the below is just an optimization for the JIT (hopefully).
    // however, conditionals must always be considered
    else if (super.getOutputPorts().isEmpty())
      return new FunctionalSchemaMatching.FunctionalTargetSchemaMatcher(functionalOperator, ohuaOperator);
    else
      return new FunctionalSchemaMatching.SchemaMatcher(functionalOperator, ohuaOperator);
  }
}
