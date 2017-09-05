/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

/**
 * This enum contains real system events such as for example the finalization of an analysis
 * step.
 * @author sertel
 * 
 */
public enum EngineEvents
{
  CYCLE_DETECTION_GRAPH_ANALYSIS_FINISHED,
  FINISHED_PROCESSING, OPERATOR_INIT_DONE
}
