"""
Programa principal de Pulumi para desplegar la infraestructura del proyecto DBT.
"""
import os
import pulumi
import pulumi_docker as docker
from pulumi_gcp import serviceaccount, projects, artifactregistry, storage
from pulumi_gcp import config as gcp_config, workflows
import yaml
import glob

# Importar módulos propios
from pulumi_resources.bigquery import create_medallion_datasets
from pulumi_resources.cloud_run import create_cloud_run_service
from pulumi_resources.workflows import create_workflow, create_workflow_trigger

# Configuración del proyecto
config = pulumi.Config()
project_id = os.getenv("GCP_PROJECT_ID", config.require("project"))
location = os.getenv("GCP_REGION", config.get("region", "us-central1"))
environment = pulumi.get_stack()

# Nombre base del proyecto
base_name = "dbt-pipeline-v1"
base_name_hyphen = base_name.replace("_", "-")

# Crear la cuenta de servicio principal
dbt_service_account = serviceaccount.Account(
    f"{base_name_hyphen}-sa",
    account_id=f"{base_name_hyphen}-sa",
    display_name=f"Cuenta de servicio para {base_name}"
)

# Roles requeridos para la cuenta de servicio
roles = {
    "bigquery-admin": "roles/bigquery.admin",
    "bigquery-data-editor": "roles/bigquery.dataEditor",
    "bigquery-job-user": "roles/bigquery.jobUser",
    "storage-admin": "roles/storage.admin",
    "workflows-invoker": "roles/workflows.invoker",
    "workflows-admin": "roles/workflows.admin",
    "run-admin": "roles/run.admin",
    "service-account-user": "roles/iam.serviceAccountUser",
    "service-account-token-creator": "roles/iam.serviceAccountTokenCreator",
    "monitoring-viewer": "roles/monitoring.viewer",
}

# Asignar roles a la cuenta de servicio
iam_members = []
for role_id, role in roles.items():
    iam_members.append(projects.IAMMember(
        f"{base_name_hyphen}-{role_id}",
        project=project_id,
        role=role,
        member=dbt_service_account.email.apply(
            lambda email: f"serviceAccount:{email}"
        )
    ))

# Crear conjuntos de datos en BigQuery (arquitectura medallion)
datasets = create_medallion_datasets(
    project=project_id,
    location="US",  # BigQuery location puede ser diferente a la región del proyecto
    delete_contents_on_destroy=False
)

# Crear bucket para almacenar datos
storage_bucket = storage.Bucket(
    f"{base_name_hyphen}-bucket",
    name=f"{project_id}-{base_name_hyphen}-{environment}",
    location=location,
    force_destroy=True,
    uniform_bucket_level_access=True,
    labels={
        "environment": environment,
        "managed-by": "pulumi",
    }
)

# Crear repositorio para imágenes de Docker
docker_repo = artifactregistry.Repository(
    f"{base_name_hyphen}-repo",
    description=f"Repositorio para imágenes de {base_name}",
    format="DOCKER",
    location=location,
    repository_id=f"{base_name_hyphen}-repo",
)

# URL del repositorio
repo_url = pulumi.Output.concat(
    location, "-docker.pkg.dev/", project_id, "/", docker_repo.repository_id
)

# Carpeta de trabajos y workflows
jobs_path = "../jobs"
workflows_path = "../workflows"

# Crear flujos de trabajo desde la carpeta workflows
def deploy_workflows():
    workflows_dict = {}
    # Buscar todos los directorios de workflows
    workflow_dirs = [d for d in os.listdir(workflows_path) if os.path.isdir(os.path.join(workflows_path, d))]
    
    for workflow_dir in workflow_dirs:
        workflow_yaml_path = os.path.join(workflows_path, workflow_dir, "workflow.yaml")
        
        if os.path.exists(workflow_yaml_path):
            # Leer el contenido del archivo YAML
            with open(workflow_yaml_path, "r") as f:
                workflow_content = f.read()
            
            # Crear el workflow
            workflow = create_workflow(
                resource_name=f"workflow-{workflow_dir}",
                location=location,
                workflow_source=workflow_content,
                project=project_id,
                service_account_email=dbt_service_account.email,
                description=f"Workflow para {workflow_dir}",
                labels={
                    "environment": environment,
                    "managed-by": "pulumi",
                    "workflow-type": workflow_dir,
                }
            )
            
            # Crear un trigger programado para el workflow (diario por defecto)
            schedule = "0 5 * * *"  # Por defecto, diariamente a las 5 AM
            if workflow_dir == "dbt_run_daily":
                schedule = "0 5 * * *"  # Diariamente a las 5 AM
            elif workflow_dir == "dbt_medallion_pipeline":
                schedule = "0 1 * * *"  # Diariamente a la 1 AM
            
            trigger = create_workflow_trigger(
                resource_name=f"trigger-{workflow_dir}",
                location=location,
                project=project_id,
                workflow=workflow,
                schedule=schedule,
                service_account_email=dbt_service_account.email,
                time_zone="America/Bogota",
            )
            
            workflows_dict[workflow_dir] = {
                "workflow": workflow,
                "trigger": trigger,
            }
    
    return workflows_dict

# Desplegar workflows si la carpeta existe
if os.path.exists(workflows_path):
    deployed_workflows = deploy_workflows()
    # Exportar información de workflows
    pulumi.export("workflows", {k: v["workflow"].name for k, v in deployed_workflows.items()})

# Exportar variables para uso en otros scripts
pulumi.export("project_id", project_id)
pulumi.export("bucket_name", storage_bucket.name)
pulumi.export("service_account_email", dbt_service_account.email)
pulumi.export("bigquery_datasets", {k: v.dataset_id for k, v in datasets.items()})
pulumi.export("docker_repo_url", repo_url)

# Si estamos en un trabajo específico, usar funcionalidad de automatización de Pulumi para ese trabajo
job_name = os.getenv("CI_JOB_NAME", "")
if job_name:
    from pulumi import automation as auto
    
    # Función para construir un trabajo específico
    def build_job_program(job_dir: str):
        def job_program():
            # Leer la configuración del trabajo desde config.yaml
            with open(f"{jobs_path}/{job_dir}/config.yaml", "r") as f:
                job_config = yaml.safe_load(f)
                
            job_name = job_config["name"]
            job_schedule = job_config.get("schedule", "0 0 * * *")
            job_time_zone = job_config.get("time_zone", "America/Bogota")
            job_memory = job_config.get("memory", "512Mi")
            job_cpu = job_config.get("cpu", "1")
            job_description = job_config.get("description", "")
            
            # Construir la imagen Docker
            image = docker.Image(
                f"{job_name}-image",
                image_name=pulumi.Output.concat(repo_url, "/", job_name),
                build=docker.DockerBuildArgs(
                    context=f"{jobs_path}/{job_dir}",
                    platform="linux/amd64",
                ),
            )
            
            # Configurar Cloud Run Job para DBT
            from pulumi_gcp import cloudrunv2
            
            # Crear el Job de Cloud Run
            job = cloudrunv2.Job(
                f"{job_name}-job",
                location=location,
                template=cloudrunv2.JobTemplateArgs(
                    template=cloudrunv2.JobTemplateTemplateArgs(
                        containers=[cloudrunv2.JobTemplateTemplateContainerArgs(
                            image=image.repo_digest,
                            resources=cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                                limits={
                                    "memory": job_memory,
                                    "cpu": job_cpu
                                }
                            ),
                            env=[cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="PROJECT_ID",
                                value=project_id
                            )]
                        )],
                        max_retries=3,
                        timeout="3600s",
                        service_account=dbt_service_account.email
                    )
                )
            )
            
            # Crear Cloud Scheduler para programar el job
            from pulumi_gcp import cloudscheduler
            
            scheduler = cloudscheduler.Job(
                f"{job_name}-scheduler",
                name=f"{job_name}-scheduler",
                schedule=job_schedule,
                time_zone=job_time_zone,
                region=location,
                http_target=cloudscheduler.JobHttpTargetArgs(
                    uri=pulumi.Output.concat(
                        "https://", location, "-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/",
                        project_id, "/jobs/", job.name, ":run"
                    ),
                    http_method="POST",
                    oauth_token=cloudscheduler.JobHttpTargetOauthTokenArgs(
                        service_account_email=dbt_service_account.email
                    )
                )
            )
            
            # Exportar información
            pulumi.export("job_name", job.name)
            pulumi.export("scheduler_name", scheduler.name)
            pulumi.export("image_url", image.repo_digest)
        
        return job_program
    
    # Construir trabajo específico
    if os.path.exists(f"{jobs_path}/{job_name}"):
        stack = auto.create_or_select_stack(
            stack_name=f"{job_name}-stack",
            project_name=base_name,
            program=build_job_program(job_name)
        )
        # Ejecutar actualización
        stack.up(on_output=print) 