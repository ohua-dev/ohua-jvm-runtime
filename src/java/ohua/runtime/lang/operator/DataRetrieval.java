/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.IDone;
import ohua.lang.OneToNSupport;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * This class encapsulates the retrieval algorithm of packets from the input
 * ports. The algorithm is part of the functional operator itself; more
 * specifically the schema matcher algorithm.
 *
 * @author sertel
 */
public class DataRetrieval implements OperatorStateAccess, IDone {

  private int _pendingInput = 0;
  private List<InputDataChecker> _inPorts = new ArrayList<>();
  private List<String> _outOfContextPorts = new ArrayList<>();

  public void addOutOfContextInput(String portName) {
    _outOfContextPorts.add(portName);
  }

  protected DataRetrieval replace(DataRetrieval n){
    n._outOfContextPorts = _outOfContextPorts;
    n._inPorts = _inPorts;
    n._pendingInput = _pendingInput;
    return n;
  }

  protected final InputDataChecker createDataInputRetrieval(String inPortRef, UserOperator ohuaOp) {
    Assertion.invariant(!_inPorts.stream().anyMatch(c -> c._inPort.getPortName() == inPortRef));
    InputDataChecker checker = create(inPortRef, ohuaOp);
    _inPorts.add(checker);
    return checker;
  }

  protected InputDataChecker create(String inPortRef, UserOperator ohuaOp){
    return _outOfContextPorts.contains(inPortRef) ?
            new OutOfContextInputDataRetrieval(ohuaOp.getDataLayer().getInputPortController(inPortRef)) :
            new InputDataRetrieval(ohuaOp.getDataLayer().getInputPortController(inPortRef));
  }

  protected boolean isCallDataAvailable(){
    // TODO optimize via functional features
    if(_inPorts.isEmpty()){
      return false;
    }else {
      boolean dataAvailable = true;
      // don't even bother retrieving anything if not all ports have data
      // available
      for (int i = _pendingInput; i < _inPorts.size(); i++) {
        InputDataChecker inPort = _inPorts.get(i);
        // the call below is not idempotent! it will delete the previous
        // packet no matter if we consumed it or not! hence the
        // "pendingInput" state.
        if (inPort.isCallDataPending()) {
          // TODO say the first port said that it's done and then the second port says I don't have data. now we memorize
          //       the index of the second port and back out. the next time we come in again we start at that index. if we
          //       find data now then we issue a call although the first port does not have any data. normally this should
          //       never happen because it means that port 2 received more data than port 1. however, in order to detect
          //       engine bugs it makes sense to switch to an ENDING state and throw an error if not all ports report that
          //       ending state.
          if (inPort.hasSeenLastPacket() && inPort.isComputationComplete()) {
            // no call should be created but we still want to check the other ports to drain EOS packets.
            dataAvailable = false;
          } else {
            _pendingInput = i;
            return false;
          }
        } else {
          // data is available
          dataAvailable &= true;
        }
      }

      _pendingInput = 0;
      return dataAvailable;
    }
  }

  @Override
  public Object getState() {
    return _pendingInput;
  }

  @Override
  public void setState(Object state) {
    _pendingInput = (int) state;
  }

  @Override
  public boolean isComputationComplete() {
    for (InputDataChecker inPort : _inPorts) {
      if (!inPort.isComputationComplete()) return false;
    }
    return true;
  }

  public static class InputDataRetrieval extends InputDataChecker {

    protected InputDataRetrieval(InputPortControl inPort) {
      super(inPort);
    }

    protected boolean isCallDataPending() {
      return !_inPort.next();
    }

    protected boolean hasSeenLastPacket() {
      return _inPort.hasSeenLastPacket();
    }

    @Override
    public boolean isComputationComplete() {
      return true;
    }
  }

  public static class OutOfContextInputDataRetrieval extends InputDataChecker {

    private int _counter = 0;
    private OneToNSupport.OneToNData _currentData = null;

    protected OutOfContextInputDataRetrieval(InputPortControl inPort) {
      super(inPort);
    }

    protected boolean isCallDataPending() {
      if (_counter == 0) {
        if (_inPort.next()) {
          _currentData = (OneToNSupport.OneToNData) _inPort.getData();
          _counter = _currentData.first();
          _inPort.setData(LanguageDataFormat.LanguagePacketAccess.MATCH.name(), _currentData.second());
          _counter--;
          return _counter <= -1; // run only if there is enough data available
        } else {
          return true;
        }
      } else {
        _inPort.setData(LanguageDataFormat.LanguagePacketAccess.MATCH.name(), _currentData.second());
        _counter--;
        return false;
      }
    }

    @Override
    public boolean isComputationComplete() {
      return _counter == 0;
    }

  }

  protected abstract static class InputDataChecker implements IDone {
    protected InputPortControl _inPort = null;

    protected InputDataChecker(InputPortControl inPort) {
      _inPort = inPort;
    }

    abstract protected boolean isCallDataPending();

    protected boolean hasSeenLastPacket() {
      return _inPort.hasSeenLastPacket();
    }

  }
}
