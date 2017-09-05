package ohua.runtime.engine.daapi;


import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;

/**
 * This interface is used to hide functions not to be visible for the user.<br>
 * Later on it might be better to also disallow access by using a adapter.
 * 
 * @author sertel
 *
 */
public abstract class DataAccess implements OperatorStateAccess
{
  protected AbstractOperatorRuntime _operatorRuntime = null;
  protected DataFormat _dataFormat = null;

  public DataAccess(AbstractOperatorRuntime operatorRuntime, DataFormat dataFormat) {
    _operatorRuntime = operatorRuntime;
    _dataFormat = dataFormat;
  }

  /**
   * Transfers the currently active data packet of the specified input port and
   * transfers it to the given output port. No copy is created!
   * <p>
   * Note, since this is a copy operation, the packet will still be available among the input
   * port afterwards.
   * @param inputPortName
   * @param outputPortName
   */
  abstract public void transferInputToOutput(String inputPortName, String outputPortName);
  
  /**
   * Directly transfers data from the input to the output port.
   * @param inputPortName
   * @param outputPortName
   * @param path
   */
  abstract public void transfer(String inputPortName, String outputPortName, String path);
  
  /**
   * Creates a new copy of the currently active data packet of the specified input port and
   * transfers it to the given output port.
   * <p>
   * Note, since this is a copy operation, the packet will still be available among the input
   * port afterwards.
   * @param inputPortName
   * @param outputPortName
   */
  abstract public void copyInputToOutput(String inputPortName, String outputPortName);

  /**
   * Utilities implemented by the chosen data layer that provide comparison, simple math
   * function and conversion for the data on the stream.
   * @return
   */
  abstract public DataUtils getDataUtils();
  
  /**
   * Used to acquire access to an input port.
   * @param inputPortName
   */
  abstract public InputPortControl getInputPortController(String inputPortName);
  
  /**
   * Used to acquire access to an output port.
   * @param outputPortName
   */
  abstract public OutputPortControl getOutputPortController(String outputPortName);

}
