import json
import os
import pickle
from contextlib import contextmanager

from airflow.configuration import conf
from airflow.executors.celery_executor import app as airflow_celery_app
from airflow.models import (
    DagBag,
    DagModel,
    DagRun,
    Pool,
    TaskFail,
    TaskInstance,
    XCom
)
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State

from flask import Response

from flask_appbuilder import BaseView
from flask_appbuilder import expose

from prometheus_client import REGISTRY, generate_latest
from prometheus_client.core import GaugeMetricFamily

from sqlalchemy import and_, func


dag_bag = None


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()


def get_number_of_running_dags():
    with session_scope(Session) as session:
        return session.query(DagRun).filter(DagRun.state == State.RUNNING).count()


def get_state_of_latest_dag_run_for_each_dag():
    dag_states = {state: 0 for state in State.dag_states}

    with session_scope(Session) as session:
        latest_dag_runs_for_each_dag = (
            session.query(
                DagRun.dag_id, func.max(DagRun.execution_date).label("execution_date")
            )
            .group_by(DagRun.dag_id)
            .cte()
        )

        states_and_counts = (
            session.query(DagRun.state, func.count(DagRun.dag_id))
            .join(
                latest_dag_runs_for_each_dag,
                and_(
                    DagRun.dag_id == latest_dag_runs_for_each_dag.c.dag_id,
                    DagRun.execution_date
                    == latest_dag_runs_for_each_dag.c.execution_date,
                ),
            )
            .group_by(DagRun.state)
            .all()
        )
        for state, count in states_and_counts:
            dag_states[state] = count

    return dag_states


def get_running_task_counts_for_queues():
    global dag_bag

    if not dag_bag:
        dag_bag = DagBag()

    task_queues = [task.queue for dag in dag_bag.dags.values() for task in dag.tasks]

    running_tasks_in_queues = {queue: 0 for queue in task_queues}

    with session_scope(Session) as session:
        for queue, count in (
            session.query(TaskInstance.queue, func.count(TaskInstance.queue))
            .filter(TaskInstance.state == "running")
            .group_by(TaskInstance.queue)
            .all()
        ):
            running_tasks_in_queues[queue] = count

    return running_tasks_in_queues


def get_total_slots():
    pool_total_slots = {}

    with session_scope(Session) as session:
        for pool in session.query(Pool).all():
            pool_total_slots[pool.pool] = pool.slots

    return pool_total_slots


def get_occupied_slots():
    pool_occupied_slots = {}

    with session_scope(Session) as session:
        for pool in session.query(Pool).all():
            pool_occupied_slots[pool.pool] = pool.occupied_slots(session)

    return pool_occupied_slots


def get_open_slots():
    pool_open_slots = {}

    with session_scope(Session) as session:
        for pool in session.query(Pool).all():
            pool_open_slots[pool.pool] = pool.open_slots(session)

    return pool_open_slots


def get_celery_current_and_max_workers_by_queue():
    i = airflow_celery_app.control.inspect()
    active_queues = i.active_queues()
    stats = i.stats()

    instances_by_queue = {}
    for celery_host, data in active_queues.items():
        for queue_data in data:
            queue_name = queue_data["name"]
            instances_by_queue[queue_name] = instances_by_queue.get(queue_name, set())
            instances_by_queue[queue_name].add(celery_host)

    worker_counts_by_queue = {}
    for queue, celery_hosts in instances_by_queue.items():
        for celery_host in celery_hosts:
            worker_counts_by_queue[queue] = worker_counts_by_queue.get(queue, {})
            worker_counts_by_queue[queue]["max"] = (
                worker_counts_by_queue[queue].get("max", 0)
                + stats[celery_host].get("autoscaler", {"max": 0})["max"]
            )
            worker_counts_by_queue[queue]["current"] = (
                worker_counts_by_queue[queue].get("current", 0)
                + stats[celery_host].get("autoscaler", {"qty": 0})["qty"]
            )

        worker_counts_by_queue[queue]["available"] = (
            worker_counts_by_queue[queue]["max"]
            - worker_counts_by_queue[queue]["current"]
        )

    return worker_counts_by_queue


def get_task_state_info():
    """Number of task instances with particular state."""
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.state,
                func.count(TaskInstance.dag_id).label("value"),
            )
            .group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state)
            .subquery()
        )
        return (
            session.query(
                task_status_query.c.dag_id,
                task_status_query.c.task_id,
                task_status_query.c.state,
                task_status_query.c.value,
                DagModel.owners,
            )
            .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .all()
        )


def get_task_failure_counts():
    """Compute Task Failure Counts."""
    with session_scope(Session) as session:
        return (
            session.query(
                TaskFail.dag_id,
                TaskFail.task_id,
                func.count(TaskFail.dag_id).label("count"),
            )
            .join(
                DagModel,
                DagModel.dag_id == TaskFail.dag_id,
            )
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .group_by(
                TaskFail.dag_id,
                TaskFail.task_id,
            )
        )


def get_num_queued_tasks():
    """Number of queued tasks currently."""
    with session_scope(Session) as session:
        return (
            session.query(TaskInstance)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )


class MetricsCollector(object):
    """Metrics Collector for prometheus."""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""
        environment = os.environ.get("ENVIRONMENT", "development")

        t_running_dags = GaugeMetricFamily(
            "airflow_running_dags", "The number of DAGs running right now", labels=["environment"]
        )
        t_running_dags.add_metric([environment], get_number_of_running_dags())
        yield t_running_dags

        t_latest_dag_run_states = GaugeMetricFamily(
            "airflow_latest_dag_run_states",
            "Looking at only the latest dag run for each dag, how many runs are in each possible state",
            labels=["state", "environment"],
        )
        for state, count in get_state_of_latest_dag_run_for_each_dag().items():
            t_latest_dag_run_states.add_metric([state, environment], count)
        yield t_latest_dag_run_states

        t_queue_tasks = GaugeMetricFamily(
            "airflow_queue_running_tasks",
            "Shows the nuumber of running tasks in a particular queue",
            labels=["queue", "environment"],
        )
        for queue, count in get_running_task_counts_for_queues().items():
            t_queue_tasks.add_metric([queue, environment], count)
        yield t_queue_tasks

        t_pool_total_slots = GaugeMetricFamily(
            "airflow_pool_total_slots",
            "Shows the total number of slots for a given pool",
            labels=["pool", "environment"],
        )
        for pool, slots in get_total_slots().items():
            t_pool_total_slots.add_metric([pool, environment], slots)
        yield t_pool_total_slots

        t_pool_occupied_slots = GaugeMetricFamily(
            "airflow_pool_occupied_slots",
            "Shows the number of occupied slots for a given pool",
            labels=["pool", "environment"],
        )
        for pool, slots in get_occupied_slots().items():
            t_pool_occupied_slots.add_metric([pool, environment], slots)
        yield t_pool_occupied_slots

        t_pool_open_slots = GaugeMetricFamily(
            "airflow_pool_open_slots",
            "Shows the number of open slots for a given pool",
            labels=["pool", "environment"],
        )
        for pool, slots in get_open_slots().items():
            t_pool_open_slots.add_metric([pool, environment], slots)
        yield t_pool_open_slots

        num_queued_tasks_metric = GaugeMetricFamily(
            "airflow_num_queued_tasks",
            "Airflow Number of Queued Tasks",
            labels=["environment"]
        )

        num_queued_tasks = get_num_queued_tasks()
        num_queued_tasks_metric.add_metric([environment], num_queued_tasks)
        yield num_queued_tasks_metric

        t_celery_queue_current_workers = GaugeMetricFamily(
            "airflow_celery_queue_current_workers",
            "Current number of worker processes available to process tasks for a given queue",
            labels=["queue", "environment"],
        )
        t_celery_queue_max_workers = GaugeMetricFamily(
            "airflow_celery_queue_max_workers",
            "Maximum number of worker processes that can be process tasks for a given queue",
            labels=["queue", "environment"],
        )
        t_celery_queue_available_workers = GaugeMetricFamily(
            "airflow_celery_queue_available_workers",
            "Number of worker processes that are available for a given queue",
            labels=["queue", "environment"],
        )
        current_and_max_workers_by_queue = get_celery_current_and_max_workers_by_queue()
        for queue, worker_info in current_and_max_workers_by_queue.items():
            t_celery_queue_current_workers.add_metric([queue, environment], worker_info["current"])
            t_celery_queue_max_workers.add_metric([queue, environment], worker_info["max"])
            t_celery_queue_available_workers.add_metric(
                [queue, environment], worker_info["available"]
            )
        yield t_celery_queue_current_workers
        yield t_celery_queue_max_workers
        yield t_celery_queue_available_workers


REGISTRY.register(MetricsCollector())


class RBACMetrics(BaseView):
    route_base = "/admin/metrics/"

    @expose("/")
    def list(self):
        return Response(generate_latest(), mimetype="text")


# Metrics View for Flask app builder used in airflow with rbac enabled
RBACmetricsView = {"view": RBACMetrics(), "name": "Metrics", "category": "Public"}
ADMIN_VIEW = []
RBAC_VIEW = [RBACmetricsView]


class DataFlowPlugin(AirflowPlugin):
    name = "data_flow_metrics_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = ADMIN_VIEW
    flask_blueprints = []
    menu_links = []
    appbuilder_views = RBAC_VIEW
    appbuilder_menu_items = []
