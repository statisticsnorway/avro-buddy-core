package no.ssb.avro.convert.core;

/**
 * An interceptor that allows for overriding a value based on the value itself, and the field name to which it was applied.
 */
public interface ValueInterceptor {

    /**
     * Applied when setting a value to a corresponding field. E.g cake='chocolate' where fieldName=cake and value=chocolate.
     * @param fieldName The name of the field to assign a value.
     * @param value The intercepted value.
     * @return The actual value that will be applied to the field.
     */
    String intercept(String fieldName, String value);
}
