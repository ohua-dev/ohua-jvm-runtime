/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.points.InputPortEvents;

public class ActivationMarkerImpl extends AbstractSignalPacket implements ActivationMarker
{
  private SystemPhaseType _phaseType = null;
  
  public ActivationMarkerImpl(SystemPhaseType phaseType)
  {
    _phaseType = phaseType;
  }

  public IStreamPacket deepCopy()
  {
    // it's ok, this packet carries no state and can therefore become shared memory
    return this;
  }
  
  public SystemPhaseType getPhaseType()
  {
    return _phaseType;
  }

  @Override public InputPortEvents getEventType() {
    return InputPortEvents.ACTIVATION_PACKET_ARRIVAL;
  }
}
