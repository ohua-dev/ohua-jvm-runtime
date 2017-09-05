/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class OperatorDescriptorParser implements ContentHandler
{
  private enum DescriptorElements
  {
    OPERATOR_DESCRIPTION,
    OPERATOR_STRUCTURE,
    OPERATOR_PROPERTIES,
    INPUT_PORTS,
    OUTPUT_PORTS,
    INPUT_PORT,
    OUTPUT_PORT
  }
  
  private enum DescriptorAttributes
  {
    ID,
    DYNAMIC
  }
  
  private Stack<Object> _callStack = new Stack<Object>();
  
  public void characters(char[] arg0, int arg1, int arg2) throws SAXException
  {
    // not supported yet
  }
  
  public void endDocument() throws SAXException
  {
    // nothing
  }
  
  public void endPrefixMapping(String arg0) throws SAXException
  {
    // no support for end of prefix yet
  }
  
  public void ignorableWhitespace(char[] arg0, int arg1, int arg2) throws SAXException
  {
//    // TODO Auto-generated method stub
//    throw new UnsupportedOperationException();
  }
  
  public void processingInstruction(String arg0, String arg1) throws SAXException
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  public void setDocumentLocator(Locator arg0)
  {
    // nothing
  }
  
  public void skippedEntity(String arg0) throws SAXException
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  public void startDocument() throws SAXException
  {
    _callStack.clear();
  }
  
  public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException
  {
    String preparedName = prepareElementName(localName);
    DescriptorElements descriptorElement = DescriptorElements.valueOf(preparedName);
    switch(descriptorElement)
    {
      case OPERATOR_DESCRIPTION:
        OperatorDescription opDescription = new OperatorDescription();
        _callStack.push(opDescription);
        break;
      case INPUT_PORT:
        parsePortAttributes(atts);
        break;
      case INPUT_PORTS:
        parsePortsAttributes(atts, true);
        List<String> inputPorts = new ArrayList<String>();
        _callStack.push(inputPorts);
        break;
      case OPERATOR_PROPERTIES:
        // castor business!
        break;
      case OPERATOR_STRUCTURE:
        // nothing yet
        break;
      case OUTPUT_PORT:
        parsePortAttributes(atts);
        break;
      case OUTPUT_PORTS:
        parsePortsAttributes(atts, false);
        List<String> outputPorts = new ArrayList<String>();
        _callStack.push(outputPorts);
        break;
    }
  }
  
  private void parsePortsAttributes(Attributes atts, boolean isInput)
  {
    String attribute = DescriptorAttributes.DYNAMIC.toString().toLowerCase();
    String attributeValue = atts.getValue(attribute);
    if(attributeValue != null)
    {
      OperatorDescription description = (OperatorDescription) _callStack.peek();
      boolean dynamic = Boolean.parseBoolean(attributeValue);
      if(isInput)
      {
        description.setDyanmicInputPorts(dynamic);
      }
      else
      {
        description.setDynamicOutputPorts(dynamic);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void parsePortAttributes(Attributes atts) throws SAXException
  {
    String attribute = DescriptorAttributes.ID.toString().toLowerCase();
    String attributeValue = atts.getValue(attribute);
    List<String> definedPorts = (List<String>) _callStack.peek();
    if(definedPorts.contains(attributeValue))
    {
      throw new SAXException("IDs for ports have to be unique! Duplicate: " + attributeValue);
    }
    else
    {
      definedPorts.add(attributeValue);
    }
  }
  
  @SuppressWarnings("unchecked")
  public void endElement(String uri, String localName, String name) throws SAXException
  {
    String preparedName = prepareElementName(localName);
    DescriptorElements descriptorElement = DescriptorElements.valueOf(preparedName);
    switch(descriptorElement)
    {
      case OPERATOR_DESCRIPTION:
        // nothing
        break;
      case INPUT_PORT:
        // nothing yet
        break;
      case INPUT_PORTS:
        List<String> inputPorts = (List<String>) _callStack.pop();
        ((OperatorDescription) _callStack.peek()).setInputPorts(inputPorts);
        break;
      case OPERATOR_PROPERTIES:
        // castor business!
        break;
      case OPERATOR_STRUCTURE:
        // nothing yet
        break;
      case OUTPUT_PORT:
        // nothing yet
        break;
      case OUTPUT_PORTS:
        List<String> outputPorts = (List<String>) _callStack.pop();
        ((OperatorDescription) _callStack.peek()).setOutputPorts(outputPorts);
        break;

    }
  }
  
  private String prepareElementName(String name)
  {
    return name.replace("-", "_").toUpperCase();
  }
  
  public void startPrefixMapping(String prefix, String uri) throws SAXException
  {
    // no prefix support yet
  }
  
  public OperatorDescription getParsedOperatorDescription()
  {
    assert _callStack.size() == 1;
    return (OperatorDescription) _callStack.peek();
  }
}
