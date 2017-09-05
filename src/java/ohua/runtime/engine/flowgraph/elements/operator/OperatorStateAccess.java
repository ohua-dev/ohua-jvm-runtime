/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;


/**
 * This interface provides access to the internal state of the operator. It can be used for
 * algorithms such as checkpointing in order to assure fault tolerance but also for migrating
 * operators from one node to another inside a cluster.
 * @author sertel
 * 
 */
public interface OperatorStateAccess
{
  Object getState();
  
  void setState(Object state);
}
