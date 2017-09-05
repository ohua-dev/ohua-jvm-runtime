/*
 * Copyright (c) Sebastian Ertel 2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.test;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

// FIXME get rid of this class once the test cases have been converted
public abstract class AbstractOhuaTestCase
{
  protected static Logger _logger = Logger.getLogger("com.ohua.tests.OhuaTestCaseLogger");
  
  @Before
  public void setUp() throws Exception
  {
    _logger.log(Level.ALL,
                "------------------- BEGIN TEST --------------------------------------------------------------");
  }
  
  @After
  public void tearDown() throws Exception
  {
    _logger.log(Level.ALL,
                "-------------------- END TEST --------------------------------------------------------------");
    
    _logger.log(Level.ALL, "");
    _logger.log(Level.ALL, "");
  }
  
  @BeforeClass
  public static void startTestClass()
  {
    _logger.log(Level.ALL,
                "************************************* START TEST CLASS ******************************************");
  }
  
  @AfterClass
  public static void finishTestClass()
  {
    _logger.log(Level.ALL,
                "************************************* FINISH TEST CLASS ******************************************");
  }
}
