/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

/**
 * Created by sebastianertel on 3/23/16.
 */
public final class FunctionalDataflowOperator extends AbstractFunctionalOperator {
    @Override
    protected AbstractSchemaMatcher getSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
        if (super.getInputPorts().isEmpty())
            return new DataflowSchemaMatching.SourceDataflowSchemaMatcher(functionalOperator, ohuaOperator);
        else if(super.getOutputPorts().isEmpty())
            return new DataflowSchemaMatching.TargetDataflowSchemaMatcher(functionalOperator, ohuaOperator);
        else
            return new DataflowSchemaMatching.DataflowSchemaMatcher(functionalOperator, ohuaOperator);
    }
}
