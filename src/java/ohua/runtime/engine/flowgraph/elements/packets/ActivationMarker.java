/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

import ohua.runtime.engine.SystemPhaseType;

public interface ActivationMarker extends IMetaDataPacket
{
  SystemPhaseType getPhaseType();
}
