/*
 * Copyright (c) 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.telemetry;

/**
 * A telemetry span wrapper that hides all specific details from the underlying telemetry library.
 *
 * A default no-op implementation is provided so that telemetry may be turned off with minimal impact on the driver code.
 */
public interface TelemetrySpan extends AutoCloseable {

    /**
     * Takes this telemetry span and makes it current in the global telemetry context.
     *
     * @return
     *         an {@link AutoCloseable} telemetry scope that represents the current telemetry context.
     */
    TelemetryScope makeCurrent();

    /**
     * Adds the specified String attribute to this telemetry span.
     *
     * @param key
     *            the key for this attribute
     * @param value
     *            the value for this attribute
     */
    default void setAttribute(TelemetryAttribute key, String value) {
        // Noop.
    }

    /**
     * Adds the specified long attribute to this telemetry span.
     *
     * @param key
     *            the key for this attribute
     * @param value
     *            the value for this attribute
     */
    default void setAttribute(TelemetryAttribute key, long value) {
        // Noop.
    }

    /**
     * Sets the status code of this telemetry span as ERROR and records the stack trace of the specified exception.
     *
     * @param cause
     *            the cause for setting this span status code to ERROR
     */
    default void setError(Throwable cause) {
        // Noop.
    }

    /**
     * Marks the end of the execution of this span.
     */
    default void end() {
        // Noop.
    }

    /**
     * {@link AutoCloseable#close()} that can be used to end this span and, making it possible to create new span within the try-with-resources blocks.
     */
    @Override
    default void close() {
        // Noop.
    }

}
