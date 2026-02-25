from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.standard.operators.python import PythonOperator


def read_cluster_state() -> dict:
    """Read namespace-scoped cluster state via the Airflow Kubernetes connection."""
    hook = KubernetesHook(conn_id="kubernetes_default")
    core_v1 = hook.core_v1_client
    namespace = "airflow"

    pods = core_v1.list_namespaced_pod(namespace=namespace).items
    services = core_v1.list_namespaced_service(namespace=namespace).items
    deployments = hook.apps_v1_client.list_namespaced_deployment(namespace=namespace).items

    pod_states = [
        {
            "name": p.metadata.name,
            "phase": p.status.phase,
            "ready": any(
                cond.type == "Ready" and cond.status == "True"
                for cond in (p.status.conditions or [])
            ),
        }
        for p in pods
    ]

    result = {
        "namespace": namespace,
        "pod_count": len(pods),
        "service_count": len(services),
        "deployment_count": len(deployments),
        "pods": pod_states,
    }
    print(result)
    return result


with DAG(
    dag_id="k8s_cluster_state_basic",
    start_date=datetime(2026, 2, 24),
    schedule=None,
    catchup=False,
    tags=["kubernetes", "example"],
) as dag:
    PythonOperator(
        task_id="read_cluster_state",
        python_callable=read_cluster_state,
    )
