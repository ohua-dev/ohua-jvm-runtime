/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.exceptions;

import org.xml.sax.Locator;

public class XMLParserException extends Exception
{
  public XMLParserException(Throwable t)
  {
    super(t);
  }
  
  // TODO
  public static Locator getLocator()
  {
    return new Locator()
    {
      
      public String getSystemId()
      {
        return "";
      }
      
      public String getPublicId()
      {
        return "";
      }
      
      public int getLineNumber()
      {
        return 0;
      }
      
      public int getColumnNumber()
      {
        return 0;
      }
    };
  }
}
