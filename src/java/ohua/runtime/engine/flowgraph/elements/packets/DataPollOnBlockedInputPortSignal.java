/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.packets;

import java.io.Serializable;

import ohua.runtime.engine.points.InputPortEvents;

public class DataPollOnBlockedInputPortSignal extends AbstractPacket implements
                                                                    Serializable,
                                                                    DataPollOnBlockedInputPortEvent
{
  @Override public InputPortEvents getEventType() {
    return InputPortEvents.DATA_POLL_ON_BLOCKED_INPUT_PORT;
  }
}
