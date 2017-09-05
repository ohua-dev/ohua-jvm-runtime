/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import java.util.ArrayList;

import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.utils.parser.OperatorDescription;
import ohua.runtime.engine.utils.parser.OperatorDescriptorDeserializer;

public class FunctionalOperatorDeserializer extends OperatorDescriptorDeserializer
{
  
  public OperatorDescription deserialize(String operatorImplName) throws OperatorLoadingException {
    try {
      if(AbstractFunctionalOperator.class.isAssignableFrom(Class.forName(operatorImplName))) {
        // the design of this operator is done else where. it is specified in the function
        // signature and return types of the operator.
        OperatorDescription desc = new OperatorDescription();
        desc.setInputPorts(new ArrayList<String>());
        desc.setDyanmicInputPorts(true);
        desc.setOutputPorts(new ArrayList<String>());
        desc.setDynamicOutputPorts(true);
        return desc;
      }
      else {
        return super.deserialize(operatorImplName);
      }
    }
    catch(ClassNotFoundException e) {
      throw new OperatorLoadingException(e);
    }
  }
}
