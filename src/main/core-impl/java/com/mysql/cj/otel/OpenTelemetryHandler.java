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

package com.mysql.cj.otel;

import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;
import java.util.function.BiConsumer;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.telemetry.TelemetryHandler;
import com.mysql.cj.telemetry.TelemetrySpan;
import com.mysql.cj.telemetry.TelemetrySpanName;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

public class OpenTelemetryHandler implements TelemetryHandler {

    private static boolean otelApiAvaliable = true;
    static {
        try {
            Class.forName("io.opentelemetry.api.GlobalOpenTelemetry");
        } catch (ClassNotFoundException e) {
            otelApiAvaliable = false;
        }
    }
    private OpenTelemetry openTelemetry = null;
    private Tracer tracer = null;
    private WeakHashMap<TelemetrySpan, Span> spans = new WeakHashMap<>();
    private List<Span> linkTargets = new ArrayList<>();

    public static boolean isOpenTelemetryApiAvailable() {
        return otelApiAvaliable;
    }

    public OpenTelemetryHandler() {
        if (!isOpenTelemetryApiAvailable()) {
            throw ExceptionFactory.createException(Messages.getString("Connection.OtelApiNotFound"));
        }

        this.openTelemetry = GlobalOpenTelemetry.get();
        this.tracer = this.openTelemetry.getTracer(Constants.CJ_NAME, Constants.CJ_VERSION);
    }

    @Override
    public TelemetrySpan startSpan(TelemetrySpanName spanName, Object... args) {
        SpanBuilder spanBuilder = this.tracer.spanBuilder(spanName.getName(args)).setSpanKind(SpanKind.CLIENT);
        this.linkTargets.stream().map(Span::getSpanContext).forEach(spanBuilder::addLink);
        Span otelSpan = spanBuilder.startSpan();
        TelemetrySpan span = new OpenTelemetrySpan(otelSpan);
        this.spans.put(span, otelSpan);
        return span;
    }

    @Override
    public void addLinkTarget(TelemetrySpan span) {
        Span otelSpan = this.spans.get(span);
        if (otelSpan != null) {
            this.linkTargets.add(otelSpan);
        }
    }

    @Override
    public void removeLinkTarget(TelemetrySpan span) {
        Span otelSpan = this.spans.get(span);
        if (otelSpan != null) {
            this.linkTargets.remove(otelSpan);
        }
    }

    @Override
    public void propagateContext(BiConsumer<String, String> traceparentConsumer) {
        this.openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), traceparentConsumer, BiConsumer::accept);
    }

}
