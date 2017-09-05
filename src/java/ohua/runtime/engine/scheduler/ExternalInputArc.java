/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import java.util.concurrent.atomic.AtomicReference;

import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

public class ExternalInputArc extends Arc
{
  protected ExternalInputArc(OperatorCore apiOp)
  {
    super();
    _targetPort = new AtomicReference<InputPort>(new InputPort(apiOp));
  }
}
