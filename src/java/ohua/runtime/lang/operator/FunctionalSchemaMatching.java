/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

/**
 * Created by sertel on 3/24/16.
 */
public abstract class FunctionalSchemaMatching {

  public static class SchemaMatcher extends AbstractSchemaMatcher implements IFunctionalSchemaMatcher {

    public SchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
      super(functionalOperator, ohuaOperator);
    }

    public Object[] matchInputSchema() {
      return matchInputSchema(this);
    }

    public boolean matchOutputSchema(Object results) {
      boolean backoff = false;
      for (Object[] outRefPrep : _outPortControls) {
        OutputPortControl outPort = (OutputPortControl) outRefPrep[0];
        backoff |= super.sendResults(outPort, super.matchOutputPort(outPort, outRefPrep, results));
      }
      return backoff;
    }

  }


  public static class FunctionalSourceSchemaMatcher extends SchemaMatcher {

    // TODO state capturing needed!
    private boolean _executed = false;

    public FunctionalSourceSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
      super(functionalOperator, ohuaOperator);
    }

    public Object[] matchInputSchema() {
      if (!_executed) {
        _executed = true;
        return loadCallData();
      } else {
        return null;
      }
    }

    public boolean isCallDataAvailable() {
      boolean old = !_executed;
      _executed = true;
      return old;
    }

  }

  public static class FunctionalTargetSchemaMatcher extends SchemaMatcher {

    public FunctionalTargetSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
      super(functionalOperator, ohuaOperator);
    }

    public boolean matchOutputSchema(Object results) {
      return false;
    }
  }

}
