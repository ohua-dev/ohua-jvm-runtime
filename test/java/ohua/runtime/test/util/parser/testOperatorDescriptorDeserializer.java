/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.test.util.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.StringWriter;

import ohua.runtime.test.AbstractRegressionTestCase;
import org.junit.Assert;

import org.exolab.castor.xml.Marshaller;
import org.exolab.castor.xml.Unmarshaller;
import org.exolab.castor.xml.XMLContext;
import org.junit.Ignore;
import org.junit.Test;

import ohua.runtime.engine.utils.parser.OperatorDescription;
import ohua.runtime.engine.utils.parser.OperatorDescriptorDeserializer;

public class testOperatorDescriptorDeserializer extends AbstractRegressionTestCase {
  @Test
  public void deserializeGeneratorDescriptor() throws Throwable {
    OperatorDescriptorDeserializer deserializer = new OperatorDescriptorDeserializer();
    OperatorDescription description = deserializer.deserialize("GeneratorOperator");
    
    Assert.assertNotNull(description.getPropertiesMapping());
  }
  
  /**
   * Does not define a mapping because it does not have any parameters.
   * @throws Throwable
   */
  @Test
  public void deserializeConsumerDescriptor() throws Throwable {
    OperatorDescriptorDeserializer deserializer = new OperatorDescriptorDeserializer();
    OperatorDescription description = deserializer.deserialize("ConsumerOperator");
    
    Assert.assertNull(description.getPropertiesMapping());
  }
  
  @Test
  public void castorMappingDeserialization() throws Throwable {
    String result = runCastorTest("GeneratorTestProperties.xml", "GeneratorOperator");
    Assert.assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<castor:GeneratorProperties\n"
                        + "    xmlns:castor=\"http://castor.org/ohua/mapping/\"\n"
                        + "    amountToGenerate=\"2000\" startOffset=\"40\"/>\n", result);
  }
  
  private String runCastorTest(String propertiesTestFileName, String operator) throws Throwable {
    XMLContext context = new XMLContext();
    OperatorDescriptorDeserializer deserializer = new OperatorDescriptorDeserializer();
    OperatorDescription description = deserializer.deserialize(operator);
    
    Assert.assertNotNull(description.getPropertiesMapping());
    context.addMapping(description.getPropertiesMapping());
    
    Unmarshaller unmarshaller = context.createUnmarshaller();
    unmarshaller.setMapping(description.getPropertiesMapping());
    String pathToProperties = getTestMethodInputDirectory() + propertiesTestFileName;
    FileReader reader = new FileReader(new File(pathToProperties));
    Object properties = unmarshaller.unmarshal(reader);
    reader.close();
    
    Marshaller marshaller = context.createMarshaller();
    marshaller.setMapping(description.getPropertiesMapping());
    marshaller.setMarshalAsDocument(true);
    StringWriter out = new StringWriter();
    marshaller.setWriter(out);
    marshaller.marshal(properties);

    return out.toString();
  }
  
  @Test
  public void definedMappingOnOneLine() throws Throwable {
    OperatorDescriptorDeserializer deserializer = new OperatorDescriptorDeserializer();
    File opDescFile = new File(getTestMethodInputDirectory() + "DummyGeneratorOperatorDescriptor.xml");
    Assert.assertEquals("DummyGeneratorOperatorDescriptor.xml", opDescFile.getName());
    OperatorDescription description =
        deserializer.deserialize(new FileInputStream(opDescFile), "DummyGeneratorOperatorDescriptor");
    Assert.assertNotNull(description.getPropertiesMapping());
  }
  
  @Test
  public void castorNamespace() throws Throwable {
    String result = runCastorTest("PropertiesInCastorNamespaceTest.xml", "GeneratorOperator");
    Assert.assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<castor:GeneratorProperties\n"
                        + "    xmlns:castor=\"http://castor.org/ohua/mapping/\"\n"
                        + "    amountToGenerate=\"2000\" startOffset=\"40\"/>\n", result);
  }
  
  @Ignore
  @Test
  public void complexCastorProperties() throws Throwable {
    String result = runCastorTest("DatabaseEndpointProperties-example.xml", "DatabaseBatchWriterOperator");
    Assert.assertEquals("", result);
  }
}
