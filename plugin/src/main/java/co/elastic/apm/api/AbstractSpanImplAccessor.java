package co.elastic.apm.api;

public class AbstractSpanImplAccessor {

    public static Object getAgentImpl(Span span) {
        return ((AbstractSpanImpl) span).span;
    }
}
