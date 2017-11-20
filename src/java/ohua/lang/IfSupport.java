package ohua.lang;


public final class IfSupport {
    @defsfn
    public Object[] bool(boolean condition) {
        return new Object[] { condition, !condition };
    }
}
