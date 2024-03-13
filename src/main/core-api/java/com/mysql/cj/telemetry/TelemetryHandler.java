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

import java.util.function.BiConsumer;

/**
 * A handler for telemetry operations. Implementations must forward each operation to an underlying telemetry API or SDK.
 *
 * A default no-op implementation is provided so that telemetry may be turned off with minimal impact to the driver code.
 */
public interface TelemetryHandler {

    /**
     * Start a telemetry span. Additionally, the returned {@link TelemetrySpan} object must be made current so that it gets recorded by the telemetry
     * tracer.
     * A {@link TelemetrySpan} object must be closed in a finally block after being made current, e.g.:
     *
     * <pre>{@code
     * TelemetrySpan span = telemetryHandler.startSpan(TelemetrySpanName.PING);
     * try (TelemetryScope scope = span.makeCurrent()) {
     *     // your code goes here
     * } finally {
     *     span.end();
     * }
     * }</pre>
     *
     * @param spanName
     *            the span name that identifies this telemetry span
     * @param args
     *            arguments used for interpolating the specified span name via {@link String#format(String, Object...)}
     * @return
     *         the newly-created span object
     */
    TelemetrySpan startSpan(TelemetrySpanName spanName, Object... args);

    /**
     * Registers the specified {@link TelemetrySpan} as a link target for subsequent new spans. Spans created after will include links to all registered link
     * target spans in the order they were added.
     *
     * @param span
     *            the {@link TelemetrySpan} to be registered as a link target for subsequent new spans
     */
    default void addLinkTarget(TelemetrySpan span) {
        // Noop.
    }

    /**
     * Removes the specified span from the list of registered link targets.
     *
     * @param span
     *            the {@link TelemetrySpan} to be removed from the list of registered link targets
     */
    default void removeLinkTarget(TelemetrySpan span) {
        // Noop.
    }

    /**
     * Injects telemetry context propagation information into the specified consumer.
     *
     * @param traceparentConsumer
     *            the consumer that will process the telemetry context propagation information
     */
    default void propagateContext(BiConsumer<String, String> traceparentConsumer) {
        // Noop.
    }

    /**
     * The telemetry context propagation default key name.
     *
     * @return
     *         the default name of the key used in telemetry context propagation
     */
    default String getContextPropagationKey() {
        return "traceparent";
    }

}
