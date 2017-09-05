/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

/**
 * Created by sertel on 1/31/17.
 */
public class WorkBasedOperatorStateMachine extends AbstractOperatorStateMachine<WorkBasedOperatorRuntime>{
  protected WorkBasedOperatorStateMachine(WorkBasedOperatorRuntime runtime) {
    super(runtime);
  }
}
