/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import java.util.ArrayList;

import ohua.runtime.engine.operators.IOperatorDescriptionProvider;
import ohua.runtime.engine.operators.OperatorDescription;

public class SFNDeserializer implements IOperatorDescriptionProvider {
  
  public OperatorDescription apply(String operatorImplName) {
        OperatorDescription desc = new OperatorDescription();
        desc.setInputPorts(new ArrayList<>());
        desc.setDyanmicInputPorts(true);
        desc.setOutputPorts(new ArrayList<>());
        desc.setDynamicOutputPorts(true);
        return desc;
  }
}
