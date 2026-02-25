# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
import logging
from typing import Iterator

import pendulum

import requests
import random

from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from airflow.sdk import chain, dag, task
from airflow.traces import otel_tracer
from airflow.traces.tracer import Trace

from pprint import pformat

logger = logging.getLogger("airflow.otel_test_dag")
_REQUESTS_INSTRUMENTED = False


def instrument_requests(otel_tracer_provider):
    global _REQUESTS_INSTRUMENTED
    if _REQUESTS_INSTRUMENTED:
        return
    RequestsInstrumentor().instrument(tracer_provider=otel_tracer_provider)
    _REQUESTS_INSTRUMENTED = True


@contextmanager
def task_root_span(ti, otel_task_tracer, otel_tracer_provider) -> Iterator:
    parent_context = None

    # DIAGNOSTIC: Check if Airflow provided trace context
    if ti.context_carrier is not None:
        logger.info(f"✓ Found context_carrier: {ti.context_carrier}")
        parent_context = otel_task_tracer.extract(ti.context_carrier)
    else:
        logger.error("❌ ti.context_carrier is None! DAG run did not create root span.")
        logger.error("Check Airflow [traces] configuration: otel_on, otel_dag_run_log_event")

    tracer = trace.get_tracer("airflow.otel_test_dag", tracer_provider=otel_tracer_provider)

    # Use INTERNAL kind for task execution
    with tracer.start_as_current_span(
        f"dag.{ti.dag_id}.task.{ti.task_id}",
        context=parent_context,
        kind=SpanKind.INTERNAL,  # Changed from CONSUMER
    ) as span:
        # Add standard attributes
        span.set_attribute("airflow.dag_id", ti.dag_id)
        span.set_attribute("airflow.task_id", ti.task_id)
        span.set_attribute("airflow.run_id", ti.run_id)

        # DIAGNOSTIC: Verify trace ID was created
        span_context = span.get_span_context()
        if span_context.trace_id == 0:
            logger.error("❌ CRITICAL: Span has trace_id = 0! OpenTelemetry not initialized!")
        else:
            trace_id = format(span_context.trace_id, '032x')
            span_id = format(span_context.span_id, '016x')
            logger.info(f"✓ Trace ID: {trace_id}")
            logger.info(f"✓ Span ID: {span_id}")

        yield parent_context

@task
def task1(ti):
    logger.info("=" * 80)
    logger.info(f"Starting Task_1 - DAG: {ti.dag_id}, Run: {ti.run_id}")
    
    context_carrier = ti.context_carrier
    
    # CRITICAL DIAGNOSTIC
    if context_carrier is None:
        logger.error("❌ NO CONTEXT CARRIER - DAG tracing is broken!")
    else:
        logger.info(f"✓ Context carrier present: {context_carrier}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    tracer_provider = otel_task_tracer.get_otel_tracer_provider()

    with task_root_span(ti, otel_task_tracer, tracer_provider) as parent_context:
        # Verify current span
        current_span = trace.get_current_span()
        ctx = current_span.get_span_context()
        
        if ctx.trace_id != 0:
            logger.info(f"✓ Active trace: {format(ctx.trace_id, '032x')}")
        
        # Your existing logic continues...
        if context_carrier is not None:
            logger.info("Found ti.context_carrier: %s.", context_carrier)
            logger.info("Extracting the span context from the context_carrier.")
            with otel_task_tracer.start_child_span(
                span_name="part1_with_parent_ctx",
                parent_context=parent_context,
                component="dag",
            ) as p1_with_ctx_s:
                p1_with_ctx_s.set_attribute("using_parent_ctx", "true")
                # Some work.
                logger.info("From part1_with_parent_ctx.")

                with otel_task_tracer.start_child_span("sub_span_without_setting_parent") as sub1_s:
                    sub1_s.set_attribute("get_parent_ctx_from_curr", "true")
                    # Some work.
                    logger.info("From sub_span_without_setting_parent.")

                    otel_tracer_provider = otel_task_tracer.get_otel_tracer_provider()

                    # To use library instrumentation we have to hook up the tracer_provider.
                    # The instrumentation library must already be installed.
                    instrument_requests(otel_tracer_provider)

                    # If we don't set the parent context, it will get it like so
                    # trace.get_current_span().get_span_context()
                    # and then start_as_current_span()
                    # tracer.start_as_current_span(name="")
                    with otel_task_tracer.start_child_span(span_name="get_repos_auto_instrumentation") as auto_instr_s:
                        # Some remote request.
                        # Get a random todo
                        todo = random.randrange(0,199)
                        response = requests.get(f"https://jsonplaceholder.typicode.com/todos/{todo}")
                        logger.info("Response: %s", response.json())

                        auto_instr_s.set_attribute("test.repos_response", pformat(response.json()))

                    tracer = trace.get_tracer("trace_test.tracer", tracer_provider=tracer_provider)
                    with tracer.start_as_current_span(name="sub_span_start_as_current") as sub_curr_s:
                        sub_curr_s.set_attribute("start_as_current", "true")
                        # Some work.
                        logger.info("From sub_span_start_as_current.")

            with otel_task_tracer.start_child_span(
                span_name="part2_with_parent_ctx",
                parent_context=parent_context,
                component="dag",
            ) as p2_with_ctx_s:
                p2_with_ctx_s.set_attribute("using_parent_ctx", "true")
                # Some work.
                logger.info("From part2_with_parent_ctx.")

    logger.info("Task_1 finished.")


@task
def task2(ti):
    logger.info("=" * 80)
    logger.info(f"Starting Task_2 - DAG: {ti.dag_id}, Run: {ti.run_id}")

    context_carrier = ti.context_carrier
    
    # DIAGNOSTIC
    if context_carrier is None:
        logger.error("❌ NO CONTEXT CARRIER in task2!")
    else:
        logger.info(f"✓ Context carrier: {context_carrier}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    otel_tracer_provider = otel_task_tracer.get_otel_tracer_provider()
    instrument_requests(otel_tracer_provider)

    with task_root_span(ti, otel_task_tracer, otel_tracer_provider):
        # Option 1: Let RequestsInstrumentor handle it automatically
        # Since you've instrumented requests, it should auto-inject trace context
        res = requests.get(
            "https://monitorama-demo-test.wallace.network/space_json/",
            timeout=25
        )
        
        # Option 2: If auto-instrumentation isn't working, manually inject
        # headers = context_carrier if context_carrier else {}
        # from opentelemetry.propagate import inject
        # inject(headers)  # Adds current span context
        # res = requests.get(
        #     "https://monitorama-demo-test.wallace.network/space_json/",
        #     headers=headers,
        #     timeout=25
        # )

        logger.info(f"\n\tStatus: {res.status_code}\n\tBody: {res.text[:200]}")
    
    logger.info("Task_2 finished")
    logger.info("=" * 80)

@task
def task3(ti):
    logger.info("=" * 80)
    logger.info(f"Starting Task_3 - DAG: {ti.dag_id}, Run: {ti.run_id}")

    context_carrier = ti.context_carrier
    
    # DIAGNOSTIC
    if context_carrier is None:
        logger.error("❌ NO CONTEXT CARRIER in task2!")
    else:
        logger.info(f"✓ Context carrier: {context_carrier}")

    otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
    otel_tracer_provider = otel_task_tracer.get_otel_tracer_provider()
    instrument_requests(otel_tracer_provider)

    with task_root_span(ti, otel_task_tracer, otel_tracer_provider):
        rn = random.randrange(0, 256)
        res = requests.get(
            "https://3jqmloorwqgmwwdoabvtcxp5pu0mqftr.lambda-url.eu-west-2.on.aws/?name=Matt&x=1",
            timeout=25
        )
        
        logger.info(f"\n\tStatus: {res.status_code}\n\tBody: {res.text[:200]}")
    
    logger.info("Task_3 finished")
    logger.info("=" * 80)

@dag(
    schedule=timedelta(seconds=30),
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
)
def otel_test_dag():
    chain(task1(), task2(), task3())

otel_test_dag()
