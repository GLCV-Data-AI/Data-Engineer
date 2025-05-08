FROM python:3.11.8-slim

ENV APP_HOME /app
WORKDIR $APP_HOME

# Copiar archivos de requerimientos e instalar dependencias
COPY dbt_project/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el proyecto DBT
COPY dbt_project .

# Instalar dependencias de DBT
RUN dbt deps

# Comando para ejecutar los modelos de la capa bronze
CMD ["dbt", "run", "--select", "tag:bronze", "--profiles-dir", "profiles/", "--target", "prod"] 