/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.daapi.DataAccess;
import ohua.runtime.engine.flowgraph.elements.AbstractUniqueID;
import ohua.runtime.engine.flowgraph.elements.abstraction.GraphNode;
import ohua.runtime.engine.flowgraph.elements.operator.Arc.ArcType;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID.OperatorIDGenerator;

import java.util.*;

public final class OperatorCore implements Operator, GraphNode, OperatorStateAccess {
  // TODO once those two lists are prepared, they should be made unmodifiable!
  private final List<InputPort> _inputPorts = new LinkedList<>();
  private final List<OutputPort> _outputPorts = new LinkedList<>();
  // convenience maps
  private final Map<PortID, InputPort> _inputPortsMap = new HashMap<>();
  private final Map<PortID, OutputPort> _outputPortsMap = new HashMap<>();
  private int _level = 0;
  private OperatorID _id = OperatorIDGenerator.generateNewOperatorID();
  private String _operatorName = null;

  private DataAccess _dataLayer = null;
  private OperatorAlgorithmAdapter _operator = null;
  private String _operatorType = null;
  private List<Object> _propertyBag = new LinkedList<>();

  public OperatorCore(String operatorType) {
    _operatorType = operatorType;
  }

  protected OperatorAlgorithmAdapter getOperatorAdapter() {
    return _operator;
  }

  public void setOperatorAdapter(OperatorAlgorithmAdapter userOperator) {
    _operator = userOperator;
  }

  public AbstractOperatorAlgorithm getOperatorAlgorithm() {
    return _operator._operatorAlgorithm;
  }

  public void addInputPort(InputPort port) {
    _inputPorts.add(port);
    _inputPortsMap.put(port.getPortId(), port);
  }

  public void removeInputPort(InputPort port) {
    _inputPorts.remove(port);
    _inputPortsMap.remove(port.getPortId());
  }

  public void addOutputPort(OutputPort port) {
    _outputPorts.add(port);
    _outputPortsMap.put(port.getPortId(), port);
  }

  public int getNumInputPorts() {
    return _inputPorts.size();
  }

  public String getOperatorName() {
    return _operatorName;
  }

  public void setOperatorName(String operatorName) {
    _operatorName = operatorName;
  }

  public int getNumOutputPorts() {
    return _outputPorts.size();
  }

  // TODO make this list unmodofiable!
  public List<InputPort> getInputPorts() {
    return _inputPorts;
  }

  // TODO make this list unmodofiable!
  public List<OutputPort> getOutputPorts() {
    return _outputPorts;
  }

  public OperatorID getId() {
    return _id;
  }

  public void setId(OperatorID id) {
    _id = id;
  }

  public int getNumGraphNodeInputs() {
    return getInputPorts().size();
  }

  public int getNumGraphNodeOutputs() {
    int numOutArcs = 0;
    for(OutputPort outPort : _outputPorts) {
      numOutArcs += outPort.getOutgoingArcs().size();
    }

    return numOutArcs;
  }

  public List<Arc> getGraphNodeInputConnections() {
    List<Arc> inputs = new ArrayList<>();
    for(InputPort inPort : _inputPorts) {
      inputs.add(inPort.getIncomingArc());
    }

    return inputs;
  }

  public List<Arc> getGraphNodeOutputConnections() {
    List<Arc> outputs = new ArrayList<>();
    for(OutputPort outPort : _outputPorts) {
      outputs.addAll(outPort.getOutgoingArcs());
    }

    return outputs;
  }

  public List<OperatorCore> getAllSucceedingGraphNodes() {
    return getAllSucceedingGraphNodes(true);
  }

  public List<OperatorCore> getAllSucceedingGraphNodes(boolean includeFeedback) {
    List<OperatorCore> successors = new ArrayList<>();

    for(OutputPort outPort : _outputPorts) {
      for(Arc outArc : outPort.getOutgoingArcs()) {
        if(outArc.getType() == ArcType.FEEDBACK_EDGE && !includeFeedback) continue;
        else successors.add(outArc.getTarget());
      }
    }

    return successors;
  }

  public List<OperatorCore> getAllPreceedingGraphNodes() {
    return getAllPreceedingGraphNodes(true);
  }

  public List<OperatorCore> getAllPreceedingGraphNodes(boolean includeFeedback) {
    List<OperatorCore> predecessors = new ArrayList<>();

    for(InputPort inPort : _inputPorts) {
      if(inPort.getIncomingArc().getType() == ArcType.FEEDBACK_EDGE && !includeFeedback) continue;
      else predecessors.add(inPort.getIncomingArc().getSource());
    }

    return predecessors;
  }

  public InputPort getInputPort(PortID id) {
    return _inputPortsMap.get(id);
  }

  public OutputPort getOutputPort(PortID id) {
    return _outputPortsMap.get(id);
  }

  public int getLevel() {
    return _level;
  }

  public void setLevel(int level) {
    _level = level;
  }

  public String getID() {
    return getOperatorName() + "-" + getId().getIDInt();
  }

  public InputPort getInputPort(String portName) {
    for(InputPort inPort : _inputPorts) {
      if(inPort.getPortName().equals(portName)) {
        return inPort;
      }
    }

    throw new RuntimeException("No input port with id \"" + portName + "\" found at operator \"" + _operatorName
                               + "\"!");
  }

  public OutputPort getOutputPort(String portName) {
    for(OutputPort outPort : _outputPorts) {
      if(outPort.getPortName().equals(portName)) {
        return outPort;
      }
    }

    throw new RuntimeException("No output port with id \"" + portName + "\" found at operator \"" + _operatorName
                               + "\"!");
  }

  protected final List<InputPort> getMetaInputPorts() {
    List<InputPort> metaInputs = new ArrayList<>();
    for(InputPort inPort : getInputPorts()) {
      if(inPort.isMetaPort()) {
        metaInputs.add(inPort);
      }
    }
    return metaInputs;
  }

  protected final List<OutputPort> getMetaOutputPorts() {
    List<OutputPort> metaOutputs = new ArrayList<>();
    for(OutputPort outPort : getOutputPorts()) {
      if(outPort.isMetaPort()) {
        metaOutputs.add(outPort);
      }
    }
    return metaOutputs;
  }

  public final DataAccess getDataLayer() {
    return _dataLayer;
  }

  public void setDataLayer(DataAccess dataLayer) {
    _dataLayer = dataLayer;
  }

  public final Object getState() {
    return new Object[] { _operator.getState(),
                          _dataLayer.getState() };
  }

  public final void setState(Object state) {
    // ports state
    for(InputPort inPort : getInputPorts()) {
      inPort.initComplete();
    }

    for(OutputPort outPort : getOutputPorts()) {
      outPort.initComplete();
    }

    _operator.setState(((Object[]) state)[0]);

    // some system operators such as the process controller or the checkpoint initiator do not
    // have a data layer
    if(_dataLayer != null) {
      _dataLayer.setState(((Object[]) state)[1]);
    }
    // FIXME set in runtime
//    _state = OperatorState.WAITING_FOR_DATA;
  }

  /**
   * An operator that does not define any input ports.
   * @return
   */
  public final boolean isSystemOutputOperator() {
    return hasUserOutputPorts() && !hasUserInputPorts();
  }

  private boolean hasUserOutputPorts() {
    return getOutputPorts().size() - getMetaOutputPorts().size() > 0;
  }

  /**
   * An operator that does not define any output ports.
   * @return
   */
  public final boolean isSystemInputOperator() {
    return hasUserInputPorts() && !hasUserOutputPorts();
  }

  private boolean hasUserInputPorts() {
    return getInputPorts().size() - getMetaInputPorts().size() > 0;
  }

  public boolean isSystemComponent() {
    return _operator.isSystemComponent();
  }

  /**
   * The order in which these names are returned maps to the port registration order.
   * @return
   */
  public List<String> getInputPortNames() {
    return getPortNames(_inputPorts);
  }

  private List<String> getPortNames(List<? extends AbstractPort> ports) {
    List<String> portNames = new ArrayList<String>();
    for(AbstractPort port : ports) {
      if(!port.isMetaPort()) {
        portNames.add(port.getPortName());
      }
    }
    return portNames;
  }

  /**
   * The order in which these names are returned maps to the port registration order.
   * @return
   */
  public List<String> getOutputPortNames() {
    return getPortNames(_outputPorts);
  }

  protected final String getOperatorType() {
    return _operatorType;
  }

  @Override
  public String toString() {
    return getID();
  }

  public AbstractUniqueID getUniqueID() {
    return _id;
  }

  public void addProperty(Object property) {
    _propertyBag.add(property);
  }

  public List<Object> getProperties() {
    return _propertyBag;
  }

}
