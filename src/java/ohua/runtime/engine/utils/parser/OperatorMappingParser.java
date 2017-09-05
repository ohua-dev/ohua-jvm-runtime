/*
 * Copyright (c) Sebastian Ertel 2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils.parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import ohua.runtime.engine.exceptions.XMLParserException;
import ohua.runtime.engine.utils.FileUtils;

public class OperatorMappingParser implements ContentHandler
{
  private final static String OPERATOR_MAPPING = "ohua:operator-mapping";
  public final static String ALIAS = "alias";
  public final static String IMPLEMENTATION_TYPE = "implementation-type";
  
  private Logger _logger = Logger.getLogger(OperatorMappingParser.class.getCanonicalName());
  
  private Map<String, String> _operatorRegistry = null;
  
  public void characters(char[] ch, int start, int length) throws SAXException {
    // TODO Auto-generated method stub
    
  }
  
  public void endDocument() throws SAXException {
    _logger.entering(OperatorMappingParser.class.getCanonicalName(), "endDocument");
    
    // nothing
  }
  
  public void endElement(String uri, String localName, String name) throws SAXException {
    _logger.log(Level.ALL, "end element: " + name);
    
  }
  
  public void endPrefixMapping(String prefix) throws SAXException {
    // don't care for now
  }
  
  public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    // don't care about whitespaces
  }
  
  public void processingInstruction(String target, String data) throws SAXException {
    // TODO Auto-generated method stub
    
  }
  
  public void setDocumentLocator(Locator locator) {
    // don't care
  }
  
  public void skippedEntity(String name) throws SAXException {
    // TODO Auto-generated method stub
    
  }
  
  public void startDocument() throws SAXException {
    _logger.entering(OperatorMappingParser.class.getCanonicalName(), "startDocument");
    
    // nothing to do for now
  }
  
  public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
    _logger.log(Level.ALL, "start element: " + name);
    
    if(!name.equals(OPERATOR_MAPPING)) {
      return;
    }
    
    String alias = atts.getValue(ALIAS);
    String implType = atts.getValue(IMPLEMENTATION_TYPE);
    assertOperatorExists(implType);
    _operatorRegistry.put(alias, implType);
    _logger.log(Level.ALL, "registering operator mapping: " + alias + " => " + implType);
  }
  
  private void assertOperatorExists(String implType) {
    try {
      Class.forName(implType);
    }
    catch(ClassNotFoundException e) {
      String message = "REGISTRATION ERROR: The operator class " + implType + " does not exist!";
      _logger.log(Level.ALL, message);
      throw new RuntimeException(message);
    }
  }
  
  public void startPrefixMapping(String prefix, String uri) throws SAXException {
    // don't care for now
  }
  
  public Map<String, String> loadOperatorMappings(String filter) throws XMLParserException {
    Map<String, String> operatorRegistry = _operatorRegistry;
    _operatorRegistry = new HashMap<String, String>();
    XMLReader xmlReader = OhuaXMLParserFactory.getInstance().createXMLReader();
    xmlReader.setContentHandler(this);
    
    try {
      List<Path> registries = FileUtils.loadMetaInfFilesFromClassPath("operators**", filter);
      for(Path reg : registries)
        xmlReader.parse(new InputSource(Files.newBufferedReader(reg)));
    }
    catch(Exception e) {
      throw new XMLParserException(e);
    }
        
    Map<String, String> loadedRegistry = _operatorRegistry;
    _operatorRegistry = operatorRegistry;
    return loadedRegistry;
  }
  
  public Map<String, String> loadOperators(String registryPath) throws XMLParserException, FileNotFoundException {
    Map<String, String> operatorRegistry = _operatorRegistry;
    _operatorRegistry = new HashMap<>();
    XMLReader xmlReader = OhuaXMLParserFactory.getInstance().createXMLReader();
    xmlReader.setContentHandler(this);
    
    boolean success = false;
    try {
      // first try: class path
      Enumeration<URL> metaInfDirs = getClass().getClassLoader().getResources(registryPath);
      while(metaInfDirs.hasMoreElements()) {
        URL metaInfDir = metaInfDirs.nextElement();
        xmlReader.parse(new InputSource(metaInfDir.openStream()));
        success = true;
        break;
      }
      
      if(!success) {
        // second try: file system
        File f = new File(registryPath);
        if(f.exists()) {
//          System.out.println(registryPath);
          xmlReader.parse(new InputSource(registryPath));
          success = true;
        }
      }
    }
    catch(Exception e) {
      throw new XMLParserException(e);
    }
    
    if(success) {
      Map<String, String> loadedRegistry = _operatorRegistry;
      _operatorRegistry = operatorRegistry;
      return loadedRegistry;
    }else{
      throw new FileNotFoundException(registryPath);
    }
  }
  
}
