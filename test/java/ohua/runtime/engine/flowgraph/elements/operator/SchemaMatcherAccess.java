/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.lang.operator.AbstractSchemaMatcher;

public abstract class SchemaMatcherAccess {

    public static boolean isCallDataAvailable(AbstractSchemaMatcher matcher) {
        return matcher.isCallDataAvailable();
    }
}
