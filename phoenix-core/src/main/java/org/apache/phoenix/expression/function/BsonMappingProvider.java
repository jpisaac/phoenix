package org.apache.phoenix.expression.function;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

public class BsonMappingProvider implements MappingProvider {

    @Override
    public <T> T map(Object source, Class<T> targetType, Configuration configuration) {
        return null;
    }

    @Override
    public <T> T map(Object source, TypeRef<T> targetType, Configuration configuration) {
        return null;
    }
}
