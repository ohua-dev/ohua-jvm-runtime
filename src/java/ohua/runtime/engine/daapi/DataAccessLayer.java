/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.PacketFactory;
import ohua.runtime.engine.flowgraph.elements.operator.*;

public final class DataAccessLayer extends DataAccess {

    private DataUtils _comparisonUtils = null;

    private Map<PortID, AbstractPortController> _portControllers = new HashMap<>();

    public DataAccessLayer(AbstractOperatorRuntime operatorRuntime, DataFormat dataFormat) {
        super(operatorRuntime, dataFormat);
        _comparisonUtils = dataFormat.getComparisonUtils();
    }

    public Maybe<Object> next(InputPort port) {
        return _operatorRuntime.pollData(port);
    }

    public void transferInputToOutput(String inputPortName, String outputPortName) {
        transferInputToOutput(inputPortName, outputPortName, false);
    }

    public void copyInputToOutput(String inputPortName, String outputPortName) {
        transferInputToOutput(inputPortName, outputPortName, true);
    }

    public void transferInputToOutput(String inputPortName, String outputPortName, boolean copy) {
        PortID inPort = _operatorRuntime.getOp().getInputPort(inputPortName).getPortId();
        PortID outPort = _operatorRuntime.getOp().getOutputPort(outputPortName).getPortId();

        AbstractPortController inController = _portControllers.get(inPort);
        AbstractPortController outController = _portControllers.get(outPort);

        if (copy) {
            outController.setCurrentDataPacket(inController.getCurrentDataPacket().deepCopy());
        } else {
            outController.setCurrentDataPacket(inController.getCurrentDataPacket());
        }
    }

    public void transfer(String inputPortName, String outputPortName, String path) {
        PortID inPort = _operatorRuntime.getOp().getInputPort(inputPortName).getPortId();
        PortID outPort = _operatorRuntime.getOp().getOutputPort(outputPortName).getPortId();

        InputPortController inController = (InputPortController) _portControllers.get(inPort);
        OutputPortController outController = (OutputPortController) _portControllers.get(outPort);

        List<Object> data = inController.getData(path);
        if (data.size() == 1) {
            outController.setData(path, data.get(0));
        } else {
            outController.setData(path, data);
        }
    }

    public boolean send(OutputPort outPort, DataPacket packet) {
        return _operatorRuntime.pushData(outPort, packet);
    }

    public boolean broadcast(DataPacket packet) {
        return _operatorRuntime.broadcast(packet);
    }

    public DataUtils getDataUtils() {
        return _comparisonUtils;
    }

    @Override
    public Object getState() {
        Map<PortID, Object> state = new HashMap<>();
        for (Map.Entry<PortID, AbstractPortController> controller : _portControllers.entrySet()) {
            state.put(controller.getKey(), controller.getValue().getState());
        }
        return state;
    }

    @Override
    public void setState(Object state) {
        @SuppressWarnings("unchecked") Map<PortID, Object> controllerState = (Map<PortID, Object>) state;
        for (Map.Entry<PortID, Object> entry : controllerState.entrySet()) {
            _portControllers.get(entry.getKey()).setState(entry.getValue());
        }
    }

    public OhuaDataAccessor getNewDataAccessor() {
        return _dataFormat.createDataAccessor();
    }

    public DataPacket newEmptyDataPacket() {
        return PacketFactory.createDataPacket(_dataFormat.createDataPacket());
    }

    public InputPortControl getInputPortController(String inputPortName) {
        PortID portID = _operatorRuntime.getOp().getInputPort(inputPortName).getPortId();
        if (!_portControllers.containsKey(portID)) {
            _portControllers.put(portID, new InputPortController(portID, this));
        }

        return (InputPortControl) _portControllers.get(portID);
    }

    public OutputPortControl getOutputPortController(String outputPortName) {
        OutputPort outPort = _operatorRuntime.getOp().getOutputPort(outputPortName);
        PortID portID = outPort.getPortId();
        if (!_portControllers.containsKey(portID)) {
            try {
                _portControllers.put(portID, outPort.getOutputPortControllerType().getConstructor(PortID.class, DataAccessLayer.class).newInstance(portID, this));
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException e) {
                Assertion.impossible(e);
            }
        }

        return (OutputPortControl) _portControllers.get(portID);
    }

    protected boolean hasSeenLastPacket(PortID portID) {
        return _operatorRuntime.getOp().getInputPort(portID).hasSeenLastPacket();
    }

    protected String getInputPortName(PortID portID) {
        return _operatorRuntime.getOp().getInputPort(portID).getPortName();
    }

    protected String getOutputPortName(PortID portID) {
        return _operatorRuntime.getOp().getOutputPort(portID).getPortName();
    }

    public OutputPort getOutputPort(PortID portID) {
        return _operatorRuntime.getOp().getOutputPort(portID);
    }

    public InputPort getInputPort(PortID portID) {
        return _operatorRuntime.getOp().getInputPort(portID);
    }
}
