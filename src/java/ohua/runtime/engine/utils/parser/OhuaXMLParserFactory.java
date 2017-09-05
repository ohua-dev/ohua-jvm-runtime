/*
 * Copyright (c) Sebastian Ertel 2008 - 2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils.parser;

import java.net.URL;
import java.util.Enumeration;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.validation.SchemaFactory;

import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.codehaus.stax2.validation.XMLValidationSchemaFactory;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import ohua.runtime.engine.exceptions.XMLParserException;

/**
 * Beware: This class is not thread-safe.
 * @author sebastian
 * 
 */
public class OhuaXMLParserFactory {
  public enum DescriptionType {
    FLOW {
      @Override
      String getSchema() {
        return "OhuaFlow.xsd";
      }
    },
    OPERATOR {
      @Override
      String getSchema() {
        return "OperatorDescriptor.xsd";
      }
    },
    REGISTRY {
      @Override
      String getSchema() {
        return "OhuaOperatorRegistry.xsd";
      }
    };
    
    abstract String getSchema();
  }
  
  private SAXParserFactory _parserFactory = null;
  private Map<DescriptionType, XMLValidationSchema> _schemas = null;
  
  private static OhuaXMLParserFactory _factory = new OhuaXMLParserFactory();
  
  private OhuaXMLParserFactory() {
    // singleton
  }
  
  public static OhuaXMLParserFactory getInstance() throws XMLParserException {
    _factory.initialize();
    return _factory;
  }
  
  private void initialize() throws XMLParserException {
    if(_parserFactory != null) {
      // has already been initialized
      return;
    }
    
    _parserFactory = SAXParserFactory.newInstance();
    _parserFactory.setNamespaceAware(true);
    
    // validation
//    SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
//    loadSchemas(schemaFactory);
  }
  
  @SuppressWarnings("unused")
  private void loadSchemas(SchemaFactory schemaFactory) throws XMLParserException {
    XMLValidationSchemaFactory sf =
        XMLValidationSchemaFactory.newInstance(XMLValidationSchema.SCHEMA_ID_W3C_SCHEMA);
    
    try {
      for(DescriptionType schema : DescriptionType.values()) {
        Enumeration<URL> resourceDirs =
            getClass().getClassLoader().getResources("META-INF/schemas/" + schema.getSchema());
        while(resourceDirs.hasMoreElements()) {
          URL resourceDir = resourceDirs.nextElement();
          _schemas.put(schema, sf.createSchema(resourceDir));
          break;
        }
      }
    }
    catch(Exception e) {
      e.printStackTrace();
      throw new XMLParserException(e);
    }
  }
  
  public final XMLReader createXMLReader() throws XMLParserException {
    try {
      SAXParser newSAXParser = _parserFactory.newSAXParser();
      return newSAXParser.getXMLReader();
    }
    catch(SAXException e) {
      throw new XMLParserException(e);
    }
    catch(ParserConfigurationException e) {
      throw new XMLParserException(e);
    }
  }
  
  // TODO put validation back in
  public void validate(DescriptionType t, URL data) throws XMLStreamException {
    XMLInputFactory2 ifact = (XMLInputFactory2) XMLInputFactory.newInstance();
    XMLStreamReader2 sr = null;
    try {
      sr = ifact.createXMLStreamReader(data);
      sr.validateAgainst(_schemas.get(t));
      // validation works in a streaming manner
      while(sr.hasNext())
        sr.next();
    }
    finally {
      if(sr != null) sr.close();
    }
  }
  
}
