/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.GraphIterator;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.sections.SectionGraph;
import ohua.runtime.engine.operators.system.ProcessControlOperator;

/**
 * Definitions:<br>
 * A flow represents a DAG as described in the flow description.<br>
 * A process is a runnable flow containing a section graph, a configuration etc.
 * @author sertel
 * 
 */
public class DataFlowProcess
{
  private FlowGraph _graph = new FlowGraph();

  protected OperatorCore _processControl = null;
  protected OperatorCore _exit = null;

  private ProcessState _state = ProcessState.IDLE;
  
  private ProcessNature _processNature = ProcessNature.SOURCE_DRIVEN;
  
  private ProcessID _processID = ProcessID.ProcessIDGenerator.generateNewProcessID();
  
  private List<PortID> _eosNeeded = null;
  
  public void setGraph(FlowGraph graph)
  {
    _graph = graph;
  }

  public FlowGraph getGraph()
  {
    return _graph;
  }
 
  public Iterator<OperatorCore> iterator()
  {
    return new GraphIterator(_graph);
  }

  protected void setState(ProcessState state)
  {
    _state = state;
  }

  protected ProcessState getState()
  {
    return _state;
  }

  public void inject(LinkedList<IMetaDataPacket> packets){
    ((ProcessControlOperator) _processControl.getOperatorAlgorithm()).inject(packets);
  }

  protected void setProcessNature(ProcessNature processNature)
  {
    _processNature = processNature;
  }

  public ProcessNature getProcessNature()
  {
    return _processNature;
  }

  public ProcessID getProcessID()
  {
    return _processID;
  }
  
  public void setEOSNeeded(List<PortID> eosNeeded)
  {
    _eosNeeded = eosNeeded;
  }
  
  public List<PortID> getEOSNeeded()
  {
    return Collections.unmodifiableList(_eosNeeded);
  }
}
