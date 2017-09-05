/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.daapi.*;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by sertel on 12/19/16.
 */
public class LanguageDataAccess extends DataAccess {

  private Map<String, InputPortControl> _inControls = new HashMap<>();
  private Map<String, OutputPortControl> _outControls = new HashMap<>();

  public LanguageDataAccess(AbstractOperatorRuntime op, DataFormat dataFormat) {
    super(op, dataFormat);
    assert dataFormat == null;
  }

  @Override
  public void transferInputToOutput(String inputPortName, String outputPortName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void transfer(String inputPortName, String outputPortName, String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyInputToOutput(String inputPortName, String outputPortName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataUtils getDataUtils() {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputPortControl getInputPortController(String inputPortName) {
    if (!_inControls.containsKey(inputPortName))
      _inControls.put(inputPortName, new LanguageInputPortController(_operatorRuntime, inputPortName));
    return _inControls.get(inputPortName);
  }

  @Override
  public OutputPortControl getOutputPortController(String outputPortName) {
    if (!_outControls.containsKey(outputPortName))
      _outControls.put(outputPortName, new LanguageOutputPortController(_operatorRuntime, outputPortName));
    return _outControls.get(outputPortName);
  }

  @Override
  public Object getState() {
    // this implementation is stateless
    return null;
  }

  @Override
  public void setState(Object state) {
    // this implementation is stateless
  }
//  }

  private class LanguageInputPortController implements InputPortControl {
    private AbstractOperatorRuntime _op;
    private InputPort _inPort;
    private boolean _dataLoaded = false;
    private Object _current = null;
//    private int _count = 0;

    private LanguageInputPortController(AbstractOperatorRuntime op, String portName) {
      _op = op;
      _inPort = op.getOp().getInputPort(portName);
    }

    @Override
    public void replay(ReplayMode mode) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasSeenLastPacket() {
      return _inPort.hasSeenLastPacket();
    }

    @Override
    public boolean next() {
      Maybe<Object> d = _op.pollData(_inPort);
      assert d != null;
      _dataLoaded = d.isPresent();
      _current = d.get();
//      if(_dataLoaded) _count++;
      return _dataLoaded;
    }

    @Override
    public boolean hasData() {
      return _dataLoaded || null != _inPort.getIncomingArc().peek();
    }

    @Override
    public void buffer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void store(String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void query(String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clean() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getPortName() {
      return _inPort.getPortName();
    }

    @Override
    public String dataToString(String format) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getLeafs() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> getData(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getData() {
//      if(!_dataLoaded)
//      if(_count == 900000)
//     System.out.println(_inPort.getOwner().getOperatorName() + "->" + _inPort.getPortName() + " : "
//                + _inPort.hasSeenLastPacket() + " : "
//                + _inPort.getOwner().getInputPorts().stream().map(InputPort::hasSeenLastPacket).collect(Collectors.toList()) + " : "
//                + _count);
//      Assertion.invariant(_dataLoaded, () -> _inPort.getOwner().getOperatorName() + "->" + _inPort.getPortName() + " : "
//              + _inPort.hasSeenLastPacket() + " : "
//              + _inPort.getOwner().getInputPorts().stream().map(InputPort::getPortName).collect(Collectors.toList()) + " : "
//              + _inPort.getOwner().getInputPorts().stream().map(InputPort::hasSeenLastPacket).collect(Collectors.toList()) + " : "
//              + _inPort.getOwner().getInputPorts().stream().map(i -> i.getIncomingArc().peek().get()).collect(Collectors.toList()) + " : "
//              + _inPort.getOwner().getInputPorts().stream().map(i -> ((Maybe)i.getCurrentPacketToBeReturned()).get()).collect(Collectors.toList()) + " : "
//              + _count);
      assert _dataLoaded;
      return _current;
    }

    @Override
    public void setData(String path, Object value) {
      _current = value;
    }

  }

  private class LanguageOutputPortController implements OutputPortControl {
    private AbstractOperatorRuntime _op;
    private OutputPort _outPort;
    private Object _current;

    private LanguageOutputPortController(AbstractOperatorRuntime op, String portName) {
      _op = op;
      _outPort = op.getOp().getOutputPort(portName);
    }

    @Override
    public boolean send() {
      return _op.pushData(_outPort, _current);
    }

    @Override
    public void newPacket() {
      // not needed anymore because there are no data packets anymore.
    }

    @Override
    public void load(File file) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void parse(String data, String format) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getPortName() {
      return _outPort.getPortName();
    }

    @Override
    public String dataToString(String format) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getLeafs() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> getData(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getData() {
      return _current;
    }

    @Override
    public void setData(String path, Object value) {
      _current = value;
    }
  }
}
