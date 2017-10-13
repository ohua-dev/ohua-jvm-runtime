/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.graph;


public final class Target {
    
    public final int operator;
    public final int index;

    public Target(final int operator, final int index) {
        this.operator = operator;
        this.index = index;
    }

    public Target(final Integer operator, final Integer index) {
        this.operator = operator;
        this.index = index;
    }
}
