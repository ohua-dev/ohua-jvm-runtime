/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

import java.nio.file.Paths;

import ohua.runtime.test.AbstractRegressionTestCase;
import org.junit.Assert;
import org.junit.Test;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

public class testOperatorLibrary extends AbstractRegressionTestCase {
  
  public static class TestOperator extends UserOperator{

    @Override
    public Object getState() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setState(Object state) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void prepare() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void runProcessRoutine() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void cleanup() {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  
  /**
   * This should test the loading and serialization of the operator libraries that are not
   * shipped with the ohua.jar.
   * @throws Throwable
   */
  @Test
  public void testClasspathExtension() throws Throwable {
    // put the test META-INF folder onto the class path
    testClasspathFileLoading.TestURLClassLoader.addToClasspath(Paths.get(getTestMethodInputDirectory() + "META-INF").toAbsolutePath().normalize().toUri().toURL());
    
    // load the operator library
    OperatorFactory fac = OperatorFactory.getInstance();
    
    // make sure the new operator is in there
    Assert.assertTrue(fac.exists("testClasspathExtensionOp"));
  }
}
