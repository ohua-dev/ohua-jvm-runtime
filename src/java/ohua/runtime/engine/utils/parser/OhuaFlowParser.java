/*
 * Copyright (c) Sebastian Ertel 2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils.parser;

import java.io.FileReader;
import java.io.IOException;
import java.util.Stack;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.exolab.castor.mapping.Mapping;
import org.exolab.castor.mapping.MappingException;
import org.exolab.castor.xml.MarshalException;
import org.exolab.castor.xml.SAX2EventProducer;
import org.exolab.castor.xml.Unmarshaller;
import org.exolab.castor.xml.ValidationException;
import org.exolab.castor.xml.XMLContext;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;

import ohua.runtime.engine.DataFlowProcess;
import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.exceptions.XMLParserException;
import ohua.runtime.engine.flowgraph.DataFlowComposition;
import ohua.runtime.engine.flowgraph.elements.AbstractUniqueID;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractPort;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID.OperatorIDGenerator;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;
import ohua.runtime.engine.utils.OhuaLoggerFactory;
import ohua.runtime.engine.utils.ReflectionUtils;

public class OhuaFlowParser extends DataFlowComposition implements ContentHandler
{
  protected enum ProcessElementTags
  {
    FLOW,
    GRAPH,
    OPERATORS,
    ARCS,
    OPERATOR,
    ARC,
    PROPERTY,
    SOURCE,
    TARGET
  }
  
  enum OperatorAttributes
  {
    TYPE,
    NAME,
    ID,
  }
  
  enum ArcAttributes
  {
    OPERATOR,
    PORT
  }
  
  enum PropertyAttributes
  {
    NAME
  }
  
  private Logger _logger = OhuaLoggerFactory.getLogger(getClass());
  
  private XMLContext _unmarshalContext = new XMLContext();
  private SAX2EventProducer _propertyContent = null;
  
  private final Stack<Object> _callStack = new Stack<Object>();
  
  private String _pathToFlow = null;
  
  public OhuaFlowParser(String pathToFlow)
  {
    OperatorIDGenerator.setStartValue(1);
    _pathToFlow = pathToFlow;
  }
  
  protected final Object getCurrentStackTop()
  {
    return _callStack.peek();
  }
  
  public void characters(char[] arg0, int arg1, int arg2) throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(), "characters", new Object[] { arg0,
                                                                                arg1,
                                                                                arg2 });
    
  }
  
  public void endDocument() throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(), "endElement");
  }
  
  public void endPrefixMapping(String arg0) throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(), "endPrefixMapping", new Object[] { arg0 });
  }
  
  public void ignorableWhitespace(char[] arg0, int arg1, int arg2) throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(), "ignorableWhitespace", new Object[] { arg0,
                                                                                         arg1,
                                                                                         arg2 });
  }
  
  public void processingInstruction(String arg0, String arg1) throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(),
                     "processingInstruction",
                     new Object[] { arg0,
                                   arg1 });
  }
  
  public void setDocumentLocator(Locator arg0)
  {
    _logger.entering(getClass().getCanonicalName(), "setDocumentLocator", new Object[] { arg0 });
  }
  
  public void skippedEntity(String arg0) throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(), "skippedEntity", new Object[] { arg0 });
  }
  
  public void startDocument() throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(), "startDocument");
    
    // FIXME this needs to load the mappings for the operators too.
    if(_unmarshalContext == null)
    {
      _unmarshalContext = new XMLContext();
    }
  }
  
  public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(), "startElement", new Object[] { uri,
                                                                                  localName,
                                                                                  name,
                                                                                  atts });
    
    ProcessElementTags elementTag = ProcessElementTags.valueOf(localName.toUpperCase());
    switch(elementTag)
    {
      case ARC:
        Arc arc = parseArc(atts);
        _callStack.push(arc);
        break;
      case ARCS:
        break;
      case FLOW:
        DataFlowProcess process = parseFlowTag(atts);
        _callStack.push(process);
        break;
      case GRAPH:
        FlowGraph graph = parseOperatorGraph(atts);
        _callStack.push(graph);
        break;
      case OPERATOR:
        OperatorCore operator = parseOperator(atts);
        _callStack.push(operator);
        break;
      case OPERATORS:
        // nothing to do for now
        break;
      case PROPERTY:
        String propertyName = parseProperty(atts);
        _callStack.push(propertyName);
        
        // subsequent events until the property - end event will be in the castor namespace and
        // therefore not arrive at this content handler.
        
        break;
      case SOURCE:
        AbstractPort sourcePort = parseArc(atts, false);
        _callStack.push(sourcePort);
        break;
      case TARGET:
        AbstractPort targetPort = parseArc(atts, true);
        _callStack.push(targetPort);
        break;
    }
  }
  
  public void endElement(String uri, String localName, String name) throws SAXException
  {
    _logger.entering(getClass().getCanonicalName(), "endElement", new Object[] { uri,
                                                                                localName,
                                                                                name });
    
    ProcessElementTags elementTag = ProcessElementTags.valueOf(localName.toUpperCase());
    switch(elementTag)
    {
      case ARC:
        Arc arc = (Arc) _callStack.pop();
        ((FlowGraph) _callStack.peek()).addArc(arc);
        break;
      case ARCS:
        // nothing for now
        break;
      case FLOW:
        // nothing for now
        break;
      case GRAPH:
        FlowGraph graph = (FlowGraph) _callStack.pop();
        ((DataFlowProcess) _callStack.peek()).setGraph(graph);
        break;
      case OPERATOR:
        _callStack.pop();
        break;
      case OPERATORS:
        // repopulate the operator registry as the ops were registered with the initial ID not
        // with the one stated in the flow
        ((FlowGraph) _callStack.peek()).repopulateOperatorRegistry();
        break;
      case PROPERTY:
        // deserialize the property values using castor
        Object propertyValue = parsePropertyData();
        String propertyName = (String) _callStack.pop();
        ReflectionUtils.setProperty(((OperatorCore) _callStack.peek()).getOperatorAlgorithm(),
                                    propertyName,
                                    propertyValue);
        break;
      case SOURCE:
        OutputPort outPort = (OutputPort) _callStack.pop();
        ((Arc) _callStack.peek()).setSourcePort(outPort);
        break;
      case TARGET:
        InputPort inPort = (InputPort) _callStack.pop();
        ((Arc) _callStack.peek()).setTargetPort(inPort);
        break;
    }
  }
  
  private Object parsePropertyData() throws SAXException
  {
    Unmarshaller unmarshaller = _unmarshalContext.createUnmarshaller();
    try
    {
      return unmarshaller.unmarshal(_propertyContent);
      
    }
    catch(MarshalException e)
    {
      throw new SAXException(e);
    }
    catch(ValidationException e)
    {
      throw new SAXException(e);
    }
  }
  
  private Arc parseArc(Attributes atts)
  {
    return super.createArc();
  }
  
  private AbstractPort parseArc(Attributes atts, boolean isInput)
  {
    OperatorCore operator = null;
    AbstractPort port = null;
    
    for(ArcAttributes arcAtt : ArcAttributes.values())
    {
      String value = atts.getValue(arcAtt.name().toLowerCase());
      switch(arcAtt)
      {
        case OPERATOR:
          operator = findOperator((FlowGraph) _callStack.get(1), Integer.parseInt(value));
          break;
        case PORT:
          assert operator != null;
          OperatorDescription description = OperatorFactory.getOperatorDescription(operator);
          if(isInput)
          {
            if(description.hasDynamicInputPorts()
               && !operator.getInputPortNames().contains(value))
            {
              port = OperatorDescription.addNewInputPort(operator, value);
            }
            else
            {
              port = operator.getInputPort(value);
            }
          }
          else
          {
            // TODO Make sure that there is never the same name specified for two different
            // ports (otherwise we will be having a problem here)!
            if(description.hasDynamicOutputPorts()
               && !operator.getOutputPortNames().contains(value))
            {
              port = OperatorDescription.addNewOutputPort(operator, value);
            }
            else
            {
              port = operator.getOutputPort(value);
            }
          }
          break;
      }
    }
    
    assert port != null;
    return port;
  }
  
  protected OperatorCore parseOperator(Attributes atts) throws SAXParseException
  {
    OperatorCore operator = null;
    for(OperatorAttributes opAtt : OperatorAttributes.values())
    {
      String value = atts.getValue(opAtt.name().toLowerCase());
      switch(opAtt)
      {
        case TYPE:
          operator = loadOperator(value);
          break;
        case NAME:
          assert operator != null;
          operator.setOperatorName(value);
          break;
        case ID:
          assert operator != null;
          // we tie those operators to this process at the very end of the parse
          operator.setId(new OperatorID(Integer.parseInt(value)));
          break;
      }
    }
    
    assert operator != null;
    return operator;
  }
  
  private OperatorCore loadOperator(String value) throws SAXParseException
  {
    try
    {
      OperatorCore op = super.loadOperator(value, (FlowGraph)_callStack.peek());
//      // FIXME only add it once!
//      Mapping propertiesMapping =
//          OperatorFactory.getInstance().getOperatorDescription(value).getPropertiesMapping();
//      if(propertiesMapping != null)
//      {
//        _unmarshalContext.addMapping(propertiesMapping);
//      }
      return op;
    }
    catch(OperatorLoadingException e)
    {
      throw new SAXParseException(e.getMessage(), XMLParserException.getLocator());
    }
//    catch(MappingException e)
//    {
//      throw new SAXParseException(e.getMessage(), XMLParserException.getLocator());
//    }
  }
    
  private String parseProperty(Attributes atts)
  {
    for(PropertyAttributes propAtt : PropertyAttributes.values())
    {
      String value = atts.getValue(propAtt.name().toLowerCase());
      switch(propAtt)
      {
        case NAME:
          return value;
          
      }
    }
    
    assert false;
    return null;
  }
  
  private FlowGraph parseOperatorGraph(Attributes atts)
  {
    return super.createGraph();
  }
  
  private DataFlowProcess parseFlowTag(Attributes atts)
  {
    return super.createProcess();
  }
  
  public void startPrefixMapping(String arg0, String arg1) throws SAXException
  {
    // TODO Auto-generated method stub
    
  }
  
  public DataFlowProcess getDeserializedProcess()
  {
    assert _callStack.size() == 1;
    return (DataFlowProcess) _callStack.pop();
  }
  
  public void setPropertySupport(SAX2EventProducer castorBuffer)
  {
    _propertyContent = castorBuffer;
  }
  
  public DataFlowProcess load() throws SAXException,
                                               ParserConfigurationException,
                                               IOException,
                                               XMLParserException
  {
    FileReader fileReader = new FileReader(_pathToFlow);
    DataFlowProcess process = parse(new InputSource(fileReader));
    unifyOperatorIDs(process.getGraph(), process.getProcessID());
    
    // since we take over the IDs stated in the flow spec, we have to update the
    // OperatorIDCounter accordingly
    OperatorIDGenerator.setStartValue(process.getGraph().getHighestOperatorID() + 1);
    
    fileReader.close();
    return process;
  }
  
  public static void unifyOperatorIDs(FlowGraph graph, AbstractUniqueID scope)
  {
    for(OperatorCore op : graph.getContainedGraphNodes())
    {
      op.getId().associate(scope);
    }
    graph.repopulateOperatorRegistry();
  }
  
  public DataFlowProcess parse(InputSource source) throws SAXException,
                                                  ParserConfigurationException,
                                                  IOException,
                                                  XMLParserException
  {
    NamespaceDispatcher nsDispatcher = new NamespaceDispatcher();
    
    SAXEventBuffer castorBuffer = new SAXEventBuffer();
    setPropertySupport(castorBuffer);
    
    nsDispatcher.register("http://www.ohua.com/ohua/OhuaFlow", this);
    nsDispatcher.register("http://castor.org/ohua/mapping/", castorBuffer);
    
    XMLReader xmlParser = OhuaXMLParserFactory.getInstance().createXMLReader();
    xmlParser.setContentHandler(nsDispatcher);
    
    xmlParser.parse(source);
    
    return getDeserializedProcess();
  }
  
  protected final OperatorCore findOperator(FlowGraph graph, int id) {
    return graph.getOperator(new OperatorID(id));
  }

  
}
