/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * Filters everything that is not in the particular namespace.
 * @author sebastian
 * 
 */
// TODO extend to also catch the prefix declarations in the xml.
public class NamespaceDispatcher extends XMLFilterImpl
{
  private Map<String, ContentHandler> _contentHandlers = new HashMap<String, ContentHandler>();
  
  private Stack<ContentHandler> _currentHandler = new Stack<ContentHandler>();
  
  @Override
  public void startDocument() throws SAXException
  {
    super.startDocument();
    for(ContentHandler handler : _contentHandlers.values())
    {
      handler.startDocument();
    }
  }
  
  @Override
  public void endDocument() throws SAXException
  {
    super.endDocument();
    for(ContentHandler handler : _contentHandlers.values())
    {
      handler.endDocument();
    }
  }
  
  @Override
  public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException
  {
    ContentHandler currentHandler = null;
    if(_contentHandlers.containsKey(uri))
    {
      currentHandler = _contentHandlers.get(uri);
    }
    else
    {
      // push it to the default handler
      currentHandler = getContentHandler();
    }
    _currentHandler.push(currentHandler);
    _currentHandler.peek().startElement(uri, localName, name, atts);
  }
  
  @Override
  public void characters(char ch[], int start, int length) throws SAXException
  {
    String s = new String(ch, start, length);
    s = s.trim();
    _currentHandler.peek().characters(s.toCharArray(), 0, s.length());
  }
  
  @Override
  public void ignorableWhitespace(char ch[], int start, int length) throws SAXException
  {
    _currentHandler.peek().ignorableWhitespace(ch, start, length);
  }
  
  @Override
  public void processingInstruction(String target, String data) throws SAXException
  {
    _currentHandler.peek().processingInstruction(target, data);
  }

  @Override
  public void endElement(String uri, String localName, String name) throws SAXException
  {
    if(_currentHandler.isEmpty())
    {
      throw new RuntimeException("XML document invalid: End element with missing start element detected!");
    }
    
    ContentHandler currentHandler = _currentHandler.pop();
    
    // validation
    boolean currentIsDefault = currentHandler == getContentHandler();
    if(currentIsDefault)
    {
      if(_contentHandlers.containsKey(uri))
      {
        throw new RuntimeException("Namespace switch during start and end element not allowed!");
      }
    }
    else
    {
      if(_contentHandlers.get(uri) != currentHandler)
      {
        throw new RuntimeException("Namespace switch during start and end element not allowed!");
      }
      
    }
    
    currentHandler.endElement(uri, localName, name);    
  }
  
  public void register(String namespace, ContentHandler handler)
  {
    if(_contentHandlers.containsKey(namespace))
    {
      throw new RuntimeException("Mapping already exists!");
    }
    _contentHandlers.put(namespace, handler);
  }
  
  public ContentHandler unregister(String namespace)
  {
    return _contentHandlers.remove(namespace);
  }
  
}
