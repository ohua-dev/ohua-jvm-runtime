/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import ohua.runtime.lang.operator.DataflowFunction;

/**
 * Created by sertel on 9/9/16.
 */
public class SelectNonStrict {

  @defsfn
  @DataflowFunction
  public Continuations select(boolean conditionWasTrue,
                              NonStrict<?> trueBranchResult,
                              NonStrict<?> falseBranchResult) {
    return conditionWasTrue ?
            Continuations.empty().at(trueBranchResult, Continuations.passThrough()) :
            Continuations.empty().at(falseBranchResult, Continuations.passThrough());
  }
}
