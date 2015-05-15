
package com.mysql.cj.api.conf;

import com.mysql.cj.api.exception.ExceptionInterceptor;

public interface PropertyDefinition {

    boolean hasValueConstraints();

    boolean isRangeBased();

    String getName();

    String getAlias();

    Object getDefaultValue();

    boolean isRuntimeModifiable();

    String getDescription();

    String getSinceVersion();

    String getCategory();

    int getOrder();

    String[] getAllowableValues();

    int getLowerBound();

    int getUpperBound();

    boolean isRequired();

    Object parseObject(String value, ExceptionInterceptor exceptionInterceptor);

    void validateAllowableValues(String valueToValidate, ExceptionInterceptor exceptionInterceptor);

    RuntimeProperty createRuntimeProperty();

}
