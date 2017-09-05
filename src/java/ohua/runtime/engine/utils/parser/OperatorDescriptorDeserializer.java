/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils.parser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Enumeration;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.exolab.castor.mapping.Mapping;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.exceptions.XMLParserException;

public class OperatorDescriptorDeserializer
{
  private Logger _logger = Logger.getLogger(getClass().getName());
  
  private OperatorDescriptorParser _opDescParser = new OperatorDescriptorParser();
  
  /**
   * 
   * @param operatorImplName - fully qualified class name
   * @return
   * @throws SAXException
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws XMLParserException
   */
  public OperatorDescription deserialize(String operatorImplName) throws OperatorLoadingException {
    OperatorDescription description = null;
    try {
      // we need to find the implementation class here as the descriptor belongs to it instead
      // of the operator type
      String operatorName = operatorImplName.substring(operatorImplName.lastIndexOf(".") + 1);
      description = loadDescription(operatorName);
      
      if(description == null && operatorName.contains("$")) {
        String op = operatorName.substring(operatorName.lastIndexOf("$") + 1);
        description = loadDescription(op);
      }
    }
    catch(Exception e) {
      throw new OperatorLoadingException(e);
    }
    
    if(description == null) {
      throw new IllegalArgumentException("Descriptor for operator " + operatorImplName + " not found.");
    }
    else {
      return description;
    }
  }
  
  private OperatorDescription loadDescription(String operatorName) throws IOException, SAXException,
                                                                  ParserConfigurationException, XMLParserException
  {
    OperatorDescription description = null;
    Enumeration<URL> opDescDirs =
        getClass().getClassLoader().getResources("META-INF/operators/" + operatorName + ".xml");
    while(opDescDirs.hasMoreElements()) {
      URL opDescDir = opDescDirs.nextElement();
      description = deserialize(opDescDir.openStream(), operatorName);
      break;
    }
    
    return description;
  }
  
  public OperatorDescription deserialize(InputStream in, String opName) throws SAXException,
                                                                       ParserConfigurationException, IOException,
                                                                       XMLParserException
  
  {
    Charset charset = Charset.forName("ISO-8859-15");
    CharsetDecoder decoder = charset.newDecoder();
    
    // the end of line pattern
    Pattern linePattern = Pattern.compile(".*\r?\n");
    
    // the patterns we are looking for
    Pattern propertiesOpenTag = Pattern.compile("<ohua:operator-properties>");
    Pattern propertiesEndTag = Pattern.compile("</ohua:operator-properties>");
    
    // read the whole file in
    ByteBuffer buffer = readDescriptorFileIntoMemory(in);
    // FileInputStream opDescriptorFile = new FileInputStream(file);
    // FileChannel opDescriptorChannel = opDescriptorFile.getChannel();
    //
    // // memory map the file
    // MappedByteBuffer mappedByteBuffer =
    // opDescriptorChannel.map(FileChannel.MapMode.READ_ONLY,
    // 0,
    // (int) opDescriptorChannel.size());
    
    // turn it into a char buffer
    CharBuffer cb = decoder.decode(buffer);
    
    // find the castor mappings section
    Matcher lm = linePattern.matcher(cb);
    Matcher pm = null; // Pattern matcher
    int startIndexCastorContent = 0;
    int endIndexCastorContent = 0;
    int dataBytesSeen = 0;
//    int lines = 0;
    boolean castorContent = false;
    
    while(lm.find()) {
//      lines++;
      CharSequence cs = lm.group(); // The current line
      
      // a new matcher for the line
      if(pm == null) {
        pm = propertiesOpenTag.matcher(cs);
      }
      else {
        pm.reset(cs);
      }
      
      if(pm.find()) {
        if(!castorContent) // open tag
        {
          castorContent = true;
          dataBytesSeen += pm.end();
          startIndexCastorContent = dataBytesSeen;
          dataBytesSeen = 0;
          pm = propertiesEndTag.matcher(cs);
        }
        else
        // close tag
        {
          dataBytesSeen += pm.start() - 1;
          endIndexCastorContent = startIndexCastorContent + dataBytesSeen;
          break;
        }
      }
      else {
        dataBytesSeen += cs.length();
      }
      
      if(lm.end() == cb.limit()) {
        _logger.fine("No properties found in operator descriptor " + opName);
        break;
      }
    }
    
    _logger.fine("part 1: " + cb.subSequence(0, startIndexCastorContent));
    _logger.fine("part 2: " + cb.subSequence(startIndexCastorContent, endIndexCastorContent));
    _logger.fine("part 3: " + cb.subSequence(endIndexCastorContent, cb.length()));
    
    ByteBuffer xmlReaderBuffer =
        ByteBuffer.allocate(cb.length() - (endIndexCastorContent - startIndexCastorContent));
    ByteBuffer castorBuffer = ByteBuffer.allocate(endIndexCastorContent - startIndexCastorContent);
    
    // map the first part to the xml reader input
    buffer.position(0);
    buffer.get(xmlReaderBuffer.array(), 0, startIndexCastorContent);
    
    // map the second part to castor input
    buffer.position(startIndexCastorContent);
    buffer.get(castorBuffer.array(), 0, endIndexCastorContent - startIndexCastorContent);
    
    // map the third part to the xml reader input
    buffer.position(endIndexCastorContent);
    buffer.get(xmlReaderBuffer.array(), startIndexCastorContent, cb.length() - endIndexCastorContent);
    
    // Close the channel and the stream
    // opDescriptorChannel.close();
    
    // 2 input sources: one for the xml reader and one for castor
    ByteArrayInputStream xmlReaderInputStream = new ByteArrayInputStream(xmlReaderBuffer.array());
    ByteArrayInputStream castorInputStream = new ByteArrayInputStream(castorBuffer.array());
    
    // parse the structure
    XMLReader xmlReader = OhuaXMLParserFactory.getInstance().createXMLReader();
    xmlReader.setContentHandler(_opDescParser);
    xmlReader.parse(new InputSource(xmlReaderInputStream));
    OperatorDescription description = _opDescParser.getParsedOperatorDescription();
    
    // parse the properties (castor mappings)
    if(castorInputStream.available() > 0) {
      Mapping propertiesMapping = new Mapping();
      description.setPropertiesMapping(propertiesMapping);
      propertiesMapping.loadMapping(new InputSource(castorInputStream));
      castorInputStream.close();
    }
    
    xmlReaderInputStream.close();
    
    return description;
  }
  
  private ByteBuffer readDescriptorFileIntoMemory(InputStream in) throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    byte[] readIn = new byte[1024];
    int read = in.read(readIn);
    while(read != -1) {
      outStream.write(readIn, 0, read);
      read = in.read(readIn);
    }
    byte[] b = outStream.toByteArray();
    outStream.close();
    ByteBuffer buffer = ByteBuffer.wrap(b);
    return buffer;
  }
}
