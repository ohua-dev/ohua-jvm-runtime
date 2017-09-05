/*
 * ohua : Either.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.util;

import java.util.function.Function;

/**
 * Created by justusadam on 13/03/2017.
 */
public abstract class Either<A,B> {
    private Either() {}

    public final static class Left<A,B> extends Either<A,B> {
        public final A a;
        public Left(final A a) {
            this.a = a;
        }
    }

    public final static class Right<A,B> extends Either<A,B> {
        public final B b;
        public Right (final B b) {
            this.b = b;
        }
    }

    public <C> C either(Function<A,C> onLeft, Function<B,C> onRight) {
        if (isLeft())
            return onLeft.apply(fromLeft());
        if (isRight())
            return onRight.apply(fromRight());
        throw new IllegalStateException("Either must be left or right");
    }

    public boolean isLeft() {
        return this instanceof Left;
    }

    public boolean isRight() {
        return this instanceof Right;
    }

    public A fromLeft() {
        return ((Left<A,B>) this).a;
    }

    public B fromRight() {
        return ((Right<A,B>) this).b;
    }
}
