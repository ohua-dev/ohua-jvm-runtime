/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.utils.parser.OperatorMappingParser;

public abstract class OperatorMappingSerializer
{
  public static void store(Map<String, String> operators, String location) {
    XMLOutputFactory factory = XMLOutputFactory.newInstance();
    
    try {
      Enumeration<URL> metaInfDirs = OperatorMappingSerializer.class.getClassLoader().getResources(location);
      boolean exists = metaInfDirs.hasMoreElements();
      String file = exists ? metaInfDirs.nextElement().getFile() : location;
      File f = new File(file);
      if(!f.exists()){
        boolean success = f.createNewFile();
        Assertion.invariant(success);
      }
      XMLStreamWriter writer = factory.createXMLStreamWriter(new FileWriter(f));
      writer.writeStartDocument();
      writer.writeCharacters("\n");
      writer.writeStartElement("", "ohua:operator-registry", "");
      writer.writeNamespace("ohua", "http://www.ohua.com/ohua/OperatorRegistry");
      for(Map.Entry<String, String> entry : operators.entrySet()) {
        writer.writeCharacters("\n  ");
        writer.writeStartElement("ohua", "operator-mapping", "");
        writer.writeAttribute(OperatorMappingParser.ALIAS, entry.getKey());
        writer.writeAttribute(OperatorMappingParser.IMPLEMENTATION_TYPE, entry.getValue());
        writer.writeEndElement();
      }
      writer.writeCharacters("\n");
      writer.writeEndElement();
      writer.writeEndDocument();
      
      writer.flush();
      writer.close();
    }
    catch(XMLStreamException e) {
      e.printStackTrace();
    }
    catch(IOException e) {
      e.printStackTrace();
    }
  }
}
