/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import java.util.List;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

public interface RuntimeRestrictionAnalysis {
  public List<OperatorID[]> defineRestrictions(CompileTimeView cv, RuntimeView rv);
}
