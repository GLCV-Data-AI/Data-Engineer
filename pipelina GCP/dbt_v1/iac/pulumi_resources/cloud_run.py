"""
Módulo para crear y configurar servicios Cloud Run.
"""
from typing import Dict, Optional
import pulumi
from pulumi_gcp import cloudrunv2

def create_cloud_run_service(
    resource_name: str,
    location: str,
    image_srv_repo_digest: pulumi.Output,
    container_port: str,
    service_account_email: pulumi.Output,
    env_vars: Optional[Dict[str, str]] = None,
    memory_limit: str = "512Mi",
    cpu_limit: str = "1",
    max_instances: int = 10,
    min_instances: int = 0,
    timeout: str = "300s"
) -> cloudrunv2.Service:
    """
    Crea un servicio Cloud Run con la configuración especificada.
    
    Args:
        resource_name: Nombre del recurso
        location: Región GCP
        image_srv_repo_digest: Digest de la imagen (output de pulumi)
        container_port: Puerto del contenedor
        service_account_email: Email de la cuenta de servicio
        env_vars: Variables de entorno (opcional)
        memory_limit: Límite de memoria
        cpu_limit: Límite de CPU
        max_instances: Máximo de instancias
        min_instances: Mínimo de instancias
        timeout: Tiempo de timeout
        
    Returns:
        Servicio Cloud Run creado
    """
    if env_vars is None:
        env_vars = {}
        
    env_variables = [{"name": k, "value": v} for k, v in env_vars.items()]
    
    return cloudrunv2.Service(
        resource_name,
        location=location,
        template=cloudrunv2.ServiceTemplateArgs(
            containers=[
                cloudrunv2.ServiceTemplateContainerArgs(
                    image=image_srv_repo_digest,
                    resources=cloudrunv2.ServiceTemplateContainerResourcesArgs(
                        limits={
                            "memory": memory_limit,
                            "cpu": cpu_limit
                        }
                    ),
                    ports=[cloudrunv2.ServiceTemplateContainerPortArgs(
                        container_port=int(container_port)
                    )],
                    env_vars=env_variables,
                ),
            ],
            timeout=timeout,
            service_account=service_account_email,
            scaling=cloudrunv2.ServiceTemplateScalingArgs(
                min_instance_count=min_instances,
                max_instance_count=max_instances
            ),
        ),
    ) 