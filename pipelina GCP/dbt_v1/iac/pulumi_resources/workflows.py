"""
Módulo para crear y gestionar Workflows en GCP.
"""
from typing import Dict, Optional, List
import pulumi
from pulumi_gcp import workflows

def create_workflow(
    resource_name: str,
    location: str,
    workflow_source: str,
    project: str,
    service_account_email: pulumi.Output,
    description: Optional[str] = None,
    labels: Optional[Dict[str, str]] = None,
    tags: Optional[List[str]] = None,
) -> workflows.Workflow:
    """
    Crea un Workflow en GCP.
    
    Args:
        resource_name: Nombre del recurso
        location: Región GCP
        workflow_source: Código fuente del workflow
        project: ID del proyecto GCP
        service_account_email: Email de la cuenta de servicio
        description: Descripción del workflow
        labels: Etiquetas para el workflow
        tags: Tags para el workflow
        
    Returns:
        El workflow creado
    """
    if labels is None:
        labels = {}
        
    args = {
        "name": resource_name,
        "region": location,
        "project": project,
        "source_contents": workflow_source,
        "service_account": service_account_email,
    }
    
    if description:
        args["description"] = description
        
    if labels:
        args["labels"] = labels
    
    return workflows.Workflow(
        resource_name,
        **args
    )

def create_workflow_trigger(
    resource_name: str,
    location: str,
    project: str,
    workflow: pulumi.Output,
    schedule: str,
    service_account_email: pulumi.Output,
    time_zone: str = "America/Bogota",
) -> workflows.Trigger:
    """
    Crea un trigger programado para un workflow.
    
    Args:
        resource_name: Nombre del recurso
        location: Región GCP
        project: ID del proyecto GCP
        workflow: Workflow a ejecutar (output de pulumi)
        schedule: Expresión cron para la programación
        service_account_email: Email de la cuenta de servicio
        time_zone: Zona horaria para la programación
        
    Returns:
        El trigger creado
    """
    return workflows.Trigger(
        resource_name,
        name=resource_name,
        location=location,
        project=project,
        workflow=workflow.id,
        event_trigger=workflows.TriggerEventTriggerArgs(
            trigger_region=location,
            service_account=service_account_email,
            event_type="google.cloud.scheduler.job.v1.executed",
            pubsub_topic=None,  # Generado automáticamente
        ),
        labels={
            "environment": pulumi.get_stack(),
            "managed-by": "pulumi",
        }
    ) 