/*
 * Copyright (c) Sebastian Ertel 2012. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.flowgraph.elements.AbstractArcQueue.DataQueueIterator;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

import java.util.List;

public abstract class AbstractArcImpl {

  private Maybe<Object> _retrievedByCall = Maybe.empty();

  protected final Maybe<Object> get(Object data){

    return Maybe.value(_retrievedByCall, data);
  }

  protected final Maybe<Object> get(){
    return Maybe.empty(_retrievedByCall);
  }

  /**
   * Returns the first item in the arc. Returns null if the arc is empty.
   * @return
   */
  abstract public Maybe<Object> getData();

  /**
   * This function will enqueue a data packet into the arc. It also provides a parameter that
   * regulates the flow inside the system. The inserting operator can choose to ignore this
   * return value in order to avoid deadlocks.
   * @param dataPacket
   * @return false - stop enqueuing into this arc! (boundary reached)
   */
  abstract protected boolean enqueue(Object dataPacket);

  abstract public Maybe<Object> peek();

  abstract public void enqueueBatch(List<? extends IStreamPacket> batch);
  
  abstract protected void enqueueMetaData(IStreamPacket metaDataPacket);
  
  abstract public int getLoadEstimate();
  
  abstract public void sweep();
  
  abstract public DataQueueIterator getDataIterator();
  
  abstract public boolean isArcEmpty();
  
  abstract public void transferTo(AbstractArcImpl arcImpl);

  protected void setMinCapacity(int capacity){
    // can be overridden to tailor the created data structure.
  }
}
