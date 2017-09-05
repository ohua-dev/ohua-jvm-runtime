package ohua.runtime.engine.operators;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

public abstract class AbstractMergeOperator extends UserOperator
{
  public static class MergeOperatorProperties implements Serializable
  {
    private int _dequeueBatchSize = 50;

    public void setDequeueBatchSize(int dequeueBatchSize)
    {
      _dequeueBatchSize = dequeueBatchSize;
    }

    public int getDequeueBatchSize()
    {
      return _dequeueBatchSize;
    }
  }
  
  private MergeOperatorProperties _properties = new MergeOperatorProperties();

  protected LinkedList<InputPortControl> _openInputPorts = new LinkedList<InputPortControl>();
  protected OutputPortControl _outPortControl = null;

  protected LinkedList<InputPortControl> getOpenInputPorts()
  {
    return _openInputPorts;
  }

  @Override
  public void cleanup()
  {
    // no other resources needed
  }
  
  @Override
  public void prepare()
  {
    prepareInputPorts();
    prepareOutputPorts();
  }

  protected void prepareOutputPorts() {
    List<String> outPorts = getOutputPorts();
    Assertion.invariant(outPorts.size() == 1);
    _outPortControl = getDataLayer().getOutputPortController(outPorts.get(0));
  }

  protected void prepareInputPorts() {
    _openInputPorts.clear();
    for(String inPort : getInputPorts())
    {
      _openInputPorts.add(getDataLayer().getInputPortController(inPort));
    }
  }
  
  public void setProperties(MergeOperatorProperties properties)
  {
    _properties = properties;
  }

  public MergeOperatorProperties getProperties()
  {
    return _properties;
  }
  
  public void setState(Object checkpoint)
  {
    prepare();
  }

}
