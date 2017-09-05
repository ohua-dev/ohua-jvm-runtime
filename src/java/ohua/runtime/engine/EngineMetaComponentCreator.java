/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.GenericGraphIterator;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.sections.MetaSection;
import ohua.runtime.engine.operators.system.ProcessControlOperator;
import ohua.runtime.engine.operators.system.UserGraphEntranceOperator;
import ohua.runtime.engine.operators.system.UserGraphExitOperator;
import ohua.runtime.engine.sections.SectionGraph;

public class EngineMetaComponentCreator
{
  public enum MetaOperator
  {
    PROCESS_CONTROL("ProcessController"),
    USER_GRAPH_ENTRANCE("Entrance"),
    USER_GRAPH_EXIT("Exit");
    
    private String _name = null;
    
    private MetaOperator(String name)
    {
      _name = name;
    }
    
    public String opName()
    {
      return _name;
    }
  }
  
  private FlowGraph _graph = null;
  private SectionGraph _sections;
  
  protected EngineMetaComponentCreator(FlowGraph graph, SectionGraph sections)
  {
    _graph = graph;
    _sections = sections;
  }
  
  protected void attachMetaComponents()
  {
    // an user graph exit
    OperatorCore metaTargetOp =
        OperatorFactory.getInstance().createSystemOperatorCore(UserGraphExitOperator.class,
                                                               "UserGraphExit");
    metaTargetOp.setOperatorName(MetaOperator.USER_GRAPH_EXIT.opName());
    // FIXME this is tricky because the next operator that gets introduced will actually have
    // the very same ID. I wonder why we can't just go with the assigned ID?!
    // metaTargetOp.setId(new OperatorID(_process.getGraph().getHighestOperatorID() + 1));
    
    MetaSection metaTargetSection = new MetaSection();
    metaTargetSection.createSingleOperatorSection(metaTargetOp);
    _sections.setUserGraphExitSection(metaTargetSection);
    
    // an user graph entrance
    OperatorCore metaSourceOp =
        OperatorFactory.getInstance().createSystemOperatorCore(UserGraphEntranceOperator.class,
                                                               "UserGraphEntrance");
    metaSourceOp.setOperatorName(MetaOperator.USER_GRAPH_ENTRANCE.opName());
    metaSourceOp.setId(new OperatorID(_graph.getLowestOperatorID() - 1));
    
    MetaSection metaSourceSection = new MetaSection();
    metaSourceSection.createSingleOperatorSection(metaSourceOp);
    _sections.setUserGraphEntranceSection(metaSourceSection);
    
    // this loop here has to be deterministic in order to assign the same IDs on restart!
    GenericGraphIterator<OperatorCore> it =
        new OperatorGraphIterator(_graph.getContainedGraphNodes());
    while(it.hasNext())
    {
      OperatorCore operator = it.next();
      if(operator.getInputPorts().isEmpty())
      {
        createMetaConnectionForSourceOperator(_graph,
                                              metaSourceOp,
                                              metaSourceSection,
                                              operator);
      }
      else if(operator.getOutputPorts().isEmpty())
      {
        createMetaConnectionForTargetOperator(_graph,
                                              metaTargetOp,
                                              metaTargetSection,
                                              operator);
      }
    }
    
    _graph.addOperator(metaSourceOp);
    _graph.addOperator(metaTargetOp);
    
    // a channel for the process manager itself to steer the flow
    createProcessInput(metaSourceOp, metaSourceSection);
  }
  
  private OperatorCore createProcessInput(OperatorCore metaSourceOp,
                                                                                      MetaSection metaSourceSection)
  {
    InputPort inPort = new InputPort(metaSourceOp);
    inPort.setMetaPort(true);
    inPort.setPortName("process-control");
    metaSourceOp.addInputPort(inPort);
    
    OperatorCore processControlOp =
        OperatorFactory.getInstance().createSystemOperatorCore(ProcessControlOperator.class,
                                                               "ProcessController");
    processControlOp.setOperatorName(MetaOperator.PROCESS_CONTROL.opName());
    OutputPort outPort = processControlOp.getOutputPort("output");
    MetaSection processInputSection = new MetaSection();
    processInputSection.createSingleOperatorSection(processControlOp);
    
    Arc arc = new Arc(outPort, inPort);
//    SectionArc sectionArc = new SectionArc();
//    sectionArc.setMappedArc(arc);
//    sectionArc.setSourceSection(processInputSection);
//    sectionArc.setTargetSection(metaSourceSection);
//    _process.getSectionGraph().addSystemArc(sectionArc);
    _sections.addSystemSection(processInputSection);
    
//    processInputSection.getOutgoingArcs().add(sectionArc);
//    metaSourceSection.getIncomingArcs().add(sectionArc);
    
    return processControlOp;
  }
  
  private void createMetaConnectionForTargetOperator(FlowGraph graph,
                                                     OperatorCore metaTargetOp,
                                                     MetaSection metaTargetSection,
                                                     OperatorCore operator)
  {
    // attach a meta output port to the operator
    OutputPort outPort = new OutputPort(operator);
    outPort.setPortName("meta-output");
    outPort.setMetaPort(true);
    operator.addOutputPort(outPort);
    
    // create a new (meta) input port for the MetaTargetOperator
    InputPort inPort = new InputPort(metaTargetOp);
    inPort.setMetaPort(true);
    metaTargetOp.addInputPort(inPort);
    
    // connect the op with the MetaTargetOperator
    Arc inArc = new Arc(outPort, inPort);
    graph.addArc(inArc);
    
//    Section sourceSection = _process.getSectionGraph().findParentSection(operator.getId());
//    SectionArc sectionArc = new SectionArc();
//    _process.getSectionGraph().addSystemArc(sectionArc);
//    sectionArc.setMappedArc(inArc);
//    sectionArc.setSourceSection(sourceSection);
//    sectionArc.setTargetSection(metaTargetSection);
    
//    sourceSection.getOutgoingArcs().add(sectionArc);
//    metaTargetSection.getIncomingArcs().add(sectionArc);
  }
  
  private void createMetaConnectionForSourceOperator(FlowGraph graph,
                                                     OperatorCore metaSourceOp,
                                                     MetaSection metaSourceSection,
                                                     OperatorCore operator)
  {
    // attach a meta input port to the operator
    InputPort inPort = new InputPort(operator);
    inPort.setPortName("meta-input");
    inPort.setMetaPort(true);
    operator.addInputPort(inPort);
    
    // FIXME not necessary. just attach many arcs to the one output port this op has.
    // create a new (meta) output port for the MetaSourceOperator
    OutputPort outPort = new OutputPort(metaSourceOp);
    outPort.setMetaPort(true);
    metaSourceOp.addOutputPort(outPort);
    
    // connect the op with the MetaTargetOperator
    Arc outArc = new Arc(outPort, inPort);
    graph.addArc(outArc);
    
//    Section targetSection = _process.getSectionGraph().findParentSection(operator.getId());
//    SectionArc sectionArc = new SectionArc();
//    _process.getSectionGraph().addSystemArc(sectionArc);
//    sectionArc.setMappedArc(outArc);
//    sectionArc.setSourceSection(metaSourceSection);
//    sectionArc.setTargetSection(targetSection);
    
//    targetSection.getIncomingArcs().add(sectionArc);
//    metaSourceSection.getOutgoingArcs().add(sectionArc);
  }
  
}
