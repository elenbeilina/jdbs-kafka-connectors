package com.aqualen.xmlflattener;

public class MappingException extends RuntimeException {
    public MappingException(String exception, Throwable cause) {
        super(exception,cause);
    }
}
