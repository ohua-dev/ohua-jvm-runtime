/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

public enum InputPortEvents
{
  ACTIVATION_PACKET_ARRIVAL,
  DATA_PACKET_ARRIVAL,
  END_OF_STREAM_PACKET_ARRIVAL,
  DATA_POLL_ON_BLOCKED_INPUT_PORT,
  CONFIGURATION_PACKET_ARRIVAL,
  FORCED_SHUTDOWN_PACKET_ARRIVAL
}
