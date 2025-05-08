"""
Módulo para crear y gestionar recursos de BigQuery en GCP.
"""
from typing import Dict, List, Optional
import pulumi
from pulumi_gcp import bigquery

def create_dataset(
    resource_name: str,
    dataset_id: str,
    project: str,
    location: str = "US",
    description: Optional[str] = None,
    labels: Optional[Dict[str, str]] = None,
    default_table_expiration_ms: Optional[int] = None,
    delete_contents_on_destroy: bool = False,
) -> bigquery.Dataset:
    """
    Crea un dataset en BigQuery.
    
    Args:
        resource_name: Nombre del recurso
        dataset_id: ID del dataset
        project: ID del proyecto GCP
        location: Ubicación del dataset
        description: Descripción del dataset
        labels: Etiquetas para el dataset
        default_table_expiration_ms: Tiempo de expiración predeterminado para tablas
        delete_contents_on_destroy: Si se deben eliminar los contenidos al destruir
        
    Returns:
        El dataset creado
    """
    if labels is None:
        labels = {
            "environment": pulumi.get_stack(),
            "managed-by": "pulumi",
        }
        
    args = {
        "dataset_id": dataset_id,
        "project": project,
        "location": location,
        "delete_contents_on_destroy": delete_contents_on_destroy,
        "labels": labels,
    }
    
    if description:
        args["description"] = description
        
    if default_table_expiration_ms:
        args["default_table_expiration_ms"] = default_table_expiration_ms
    
    return bigquery.Dataset(
        resource_name,
        **args
    )

def create_medallion_datasets(
    project: str,
    location: str = "US",
    delete_contents_on_destroy: bool = False,
) -> Dict[str, bigquery.Dataset]:
    """
    Crea los datasets para la arquitectura medallion (bronce, silver, gold).
    
    Args:
        project: ID del proyecto GCP
        location: Ubicación de los datasets
        delete_contents_on_destroy: Si se deben eliminar los contenidos al destruir
        
    Returns:
        Diccionario con los datasets creados
    """
    datasets = {}
    
    # Dataset Bronze (datos crudos)
    datasets["bronze"] = create_dataset(
        resource_name="bronze-dataset",
        dataset_id="bronze",
        project=project,
        location=location,
        description="Datos crudos extraídos de las fuentes originales",
        delete_contents_on_destroy=delete_contents_on_destroy,
        labels={
            "layer": "bronze",
            "environment": pulumi.get_stack(),
            "managed-by": "pulumi",
        }
    )
    
    # Dataset Silver (datos procesados)
    datasets["silver"] = create_dataset(
        resource_name="silver-dataset",
        dataset_id="silver",
        project=project,
        location=location,
        description="Datos limpios y transformados para análisis",
        delete_contents_on_destroy=delete_contents_on_destroy,
        labels={
            "layer": "silver",
            "environment": pulumi.get_stack(),
            "managed-by": "pulumi",
        }
    )
    
    # Dataset Gold (datos de negocio)
    datasets["gold"] = create_dataset(
        resource_name="gold-dataset",
        dataset_id="gold",
        project=project,
        location=location,
        description="Datos agregados y optimizados para consumo de negocio",
        delete_contents_on_destroy=delete_contents_on_destroy,
        labels={
            "layer": "gold",
            "environment": pulumi.get_stack(),
            "managed-by": "pulumi",
        }
    )
    
    # Dataset para resultados de calidad de datos
    datasets["dq_results"] = create_dataset(
        resource_name="dq-results-dataset",
        dataset_id="dq_results",
        project=project,
        location=location,
        description="Resultados de pruebas de calidad de datos",
        delete_contents_on_destroy=delete_contents_on_destroy,
        labels={
            "type": "data_quality",
            "environment": pulumi.get_stack(),
            "managed-by": "pulumi",
        }
    )
    
    return datasets 