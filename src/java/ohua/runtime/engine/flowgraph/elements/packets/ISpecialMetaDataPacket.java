/*
 * Copyright (c) Sebastian Ertel 2012. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

/**
 * The implementation of handlers dealing with these markers needs to adhere to the following
 * guidelines:
 * <ul>
 * <li>The handler implementation should be stateless.
 * <li>The target operator for this packet should not be an operator that can be executed in
 * parallel! (TODO There is really a concept missing here that prevents that this markers get
 * consumed in parallel. But on the other hand, this might be desirable in order to interrupt an
 * operator execution!)
 * </ul>
 * @author sertel
 * 
 */
public interface ISpecialMetaDataPacket
{
  // pure marker interface for packets that are allowed to be processed in parallel to the
  // execution of an operator.
}
