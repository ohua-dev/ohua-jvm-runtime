/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.utils.parser;

import java.util.LinkedList;

import javax.xml.stream.XMLStreamConstants;

import org.exolab.castor.xml.SAX2EventProducer;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

public class SAXEventBuffer implements SAX2EventProducer, ContentHandler
{
  class SAXEvent
  {
    int _eventType = -1;
    Object[] _arguments = null;
    
    SAXEvent(int eventType, Object... args)
    {
      _eventType = eventType;
      _arguments = args;
    }
  }

  LinkedList<SAXEvent> _eventQueue = new LinkedList<SAXEvent>();
  private ContentHandler _eventReceiver = null;

  /*
   * (non-Javadoc)
   * 
   * @see org.exolab.castor.xml.SAX2EventProducer#setContentHandler(org.xml.sax.ContentHandler)
   */
  public void setContentHandler(ContentHandler arg0)
  {
    _eventReceiver = arg0;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.exolab.castor.xml.SAX2EventProducer#start()
   */
  public void start() throws SAXException
  {
    while(!_eventQueue.isEmpty())
    {
      SAXEvent event = _eventQueue.removeLast();
      switch(event._eventType)
      {
        case XMLStreamConstants.ATTRIBUTE:
          _eventReceiver.endPrefixMapping((String) event._arguments[0]);
          break;
        case XMLStreamConstants.CHARACTERS:
          _eventReceiver.characters((char[]) event._arguments[0],
                                    (Integer) event._arguments[1],
                                    (Integer) event._arguments[2]);
          break;
        case XMLStreamConstants.DTD:
          _eventReceiver.setDocumentLocator((Locator) event._arguments[0]);
          break;
        case XMLStreamConstants.END_DOCUMENT:
          _eventReceiver.endDocument();
          break;
        case XMLStreamConstants.ENTITY_DECLARATION:
          _eventReceiver.skippedEntity((String) event._arguments[0]);
          break;
        case XMLStreamConstants.NOTATION_DECLARATION:
          _eventReceiver.startPrefixMapping((String) event._arguments[0],
                                            (String) event._arguments[1]);
          break;
        case XMLStreamConstants.PROCESSING_INSTRUCTION:
          _eventReceiver.processingInstruction((String) event._arguments[0],
                                               (String) event._arguments[1]);
          break;
        case XMLStreamConstants.SPACE:
          _eventReceiver.ignorableWhitespace((char[]) event._arguments[0],
                                             (Integer) event._arguments[1],
                                             (Integer) event._arguments[2]);
          break;
        case XMLStreamConstants.START_DOCUMENT:
          _eventReceiver.startDocument();
          break;
        case XMLStreamConstants.START_ELEMENT:
          _eventReceiver.startElement((String) event._arguments[0],
                                      (String) event._arguments[1],
                                      (String) event._arguments[2],
                                      (Attributes) event._arguments[3]);
          break;
        case XMLStreamConstants.END_ELEMENT:
          _eventReceiver.endElement((String) event._arguments[0],
                                      (String) event._arguments[1],
                                      (String) event._arguments[2]);
          break;
        default:
          assert false;
      }
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#characters(char[], int, int)
   */
  public void characters(char[] arg0, int arg1, int arg2) throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.CHARACTERS, arg0, arg1, arg2));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#endDocument()
   */
  public void endDocument() throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.END_DOCUMENT));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String,
   * java.lang.String)
   */
  public void endElement(String arg0, String arg1, String arg2) throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.END_ELEMENT, arg0, arg1, arg2));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#endPrefixMapping(java.lang.String)
   */
  public void endPrefixMapping(String arg0) throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.ATTRIBUTE));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#ignorableWhitespace(char[], int, int)
   */
  public void ignorableWhitespace(char[] arg0, int arg1, int arg2) throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.SPACE, arg0, arg1, arg2));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#processingInstruction(java.lang.String, java.lang.String)
   */
  public void processingInstruction(String arg0, String arg1) throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.PROCESSING_INSTRUCTION, arg0, arg1));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#setDocumentLocator(org.xml.sax.Locator)
   */
  public void setDocumentLocator(Locator arg0)
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.DTD, arg0));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#skippedEntity(java.lang.String)
   */
  public void skippedEntity(String arg0) throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.ENTITY_DECLARATION, arg0));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#startDocument()
   */
  public void startDocument() throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.START_DOCUMENT));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String,
   * java.lang.String, org.xml.sax.Attributes)
   */
  public void startElement(String arg0, String arg1, String arg2, Attributes arg3) throws SAXException
  {
    // we need to take a snapshot of the attributes here because the underlying XMLParser reuses
    // this object for the next element with attributes!
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.START_ELEMENT,
                                      arg0,
                                      arg1,
                                      arg2,
                                      new AttributesImpl(arg3)));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.xml.sax.ContentHandler#startPrefixMapping(java.lang.String, java.lang.String)
   */
  public void startPrefixMapping(String arg0, String arg1) throws SAXException
  {
    _eventQueue.addFirst(new SAXEvent(XMLStreamConstants.NOTATION_DECLARATION, arg0, arg1));
  }
  
}
