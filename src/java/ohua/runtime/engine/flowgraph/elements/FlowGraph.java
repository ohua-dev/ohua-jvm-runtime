/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.abstraction.Graph;
import ohua.runtime.engine.flowgraph.elements.operator.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlowGraph implements Graph<OperatorCore>
{
  private List<Arc> _arcs = new LinkedList<>();
  
  private Map<PortID, AbstractPort> _portRegistry = new LinkedHashMap<>();
  
  private Map<OperatorID, OperatorCore> _operatorRegistry = new LinkedHashMap<>();
  
  private int _highestOperatorID = 0;
  private int _lowestOperatorID = 100000;
  
  public void addOperator(OperatorCore op) {
    if(_operatorRegistry.containsKey(op.getId())) {
      // we do not support overrides
      throw new UnsupportedOperationException("Attempt to override " + _operatorRegistry.get(op.getId()).getID()
                                              + " with operator " + op.getID());
    }
    
    registerOperatorPorts(op);
    _operatorRegistry.put(op.getId(), op);
    _highestOperatorID = Math.max(_highestOperatorID, op.getId().getIDInt());
    _lowestOperatorID = Math.min(_lowestOperatorID, op.getId().getIDInt());
  }
  
  private void registerOperatorPorts(OperatorCore op) {
    for(InputPort inPort : op.getInputPorts()) {
      assert !_portRegistry.containsKey(inPort.getPortId());
      _portRegistry.put(inPort.getPortId(), inPort);
    }
  }
  
  public AbstractPort getPortByID(PortID id) {
    return _portRegistry.get(id);
  }
  
  public void addArc(Arc a) {
    _arcs.add(a);
  }
  
  public List<OperatorCore> getContainedGraphNodes() {
    return Collections.unmodifiableList(new LinkedList<OperatorCore>(_operatorRegistry.values()));
  }
  
  public List<Arc> getContainedArcs() {
    return _arcs;
  }
  
  public List<Arc> getContainedArcsFromOps() {
    List<Arc> arcs = new ArrayList<Arc>();
    for(OperatorCore op : getContainedGraphNodes()) {
      arcs.addAll(op.getGraphNodeOutputConnections());
    }
    return arcs;
  }
  
  public static List<OperatorCore> getAllDownstreamNeighbors(Operator inputOperator) {
    assert inputOperator != null;
    List<OperatorCore> downstreamNeighbors = new ArrayList<>();
    
    for(OutputPort outPort : inputOperator.getOutputPorts()) {
      for(Arc outArc : outPort.getOutgoingArcs()) {
        // all flows should be valid at that point!
        assert outArc.getTarget() != null;
        downstreamNeighbors.add(outArc.getTarget());
      }
    }
    
    assert !downstreamNeighbors.isEmpty();
    return downstreamNeighbors;
  }

  public void debug() {
    System.out.println("Graph info:");
    System.out.println("Included operators:");
    for(OperatorCore op : getContainedGraphNodes()) {
      System.out.println(op.getOperatorName());
    }
  }

  public OperatorCore getOperator(String operatorName) {
    for(OperatorCore operator : _operatorRegistry.values()) {
      if(operator.getOperatorName().equals(operatorName)) {
        return operator;
      }
    }
    
    throw new RuntimeException("Operator with name \"" + operatorName + "\" not found.");
  }
  
  public OperatorCore getOperator(OperatorID opID) {
    if(!_operatorRegistry.containsKey(opID)) {
      throw new RuntimeException("Operator with ID \"" + opID + "\" not found.");
    }
    
    return _operatorRegistry.get(opID);
  }
  
  public int getHighestOperatorID() {
    return _highestOperatorID;
  }
  
  public int getLowestOperatorID() {
    return _lowestOperatorID;
  }
  
  /**
   * To be invoked after the OperatorIDs have changed.
   */
  public void repopulateOperatorRegistry() {
    _highestOperatorID = 0;
    _lowestOperatorID = 100000;
    
    HashMap<OperatorID, OperatorCore> operatorRegistry = new HashMap<>(_operatorRegistry);
    _operatorRegistry.clear();
    _portRegistry.clear();
    for(OperatorCore op : operatorRegistry.values()) {
      addOperator(op);
    }
  }
  
  public List<OperatorCore> getOperators(String regex) {
    Pattern p = Pattern.compile(regex);
    List<OperatorCore> ops = new ArrayList<>();
    for(OperatorCore op : _operatorRegistry.values()) {
      Matcher matcher = p.matcher(op.getOperatorName());
      if(matcher.matches()) {
        ops.add(op);
      }
    }
    return ops;
  }
  
  public String printOperatorList() {
    StringBuffer list = new StringBuffer();
    for(OperatorCore core : _operatorRegistry.values()) {
      list.append(core.getOperatorName());
      list.append("\n");
    }
    return list.toString();
  }

  public static String constructPortReference(AbstractPort port){
    return constructPortReference(port.getOwner().getOperatorName(), port.getPortName());
  }
  
  public static String constructPortReference(String operatorName, String portName){
    return operatorName + "." + portName;
  }
  
  public static String[] parsePortReference(String portReference) {
    if(portReference.lastIndexOf(".") < 0)
      Assertion.invariant(false, "Path was not of length 2! Should be op-name.port-name but was: " + portReference);

    String[] path = new String[] { portReference.substring(0, portReference.lastIndexOf(".")),
            portReference.substring(portReference.lastIndexOf(".") + 1)};
    return path;
  }

  public void addAll(FlowGraph graph) {
    for(OperatorCore op : graph.getContainedGraphNodes()){
      addOperator(op);
    }
    for(Arc arc : graph.getContainedArcs()){
      addArc(arc);
    }
  }
}
