#FROM jupyter/pyspark-notebook:spark-3.3.0
FROM deltaio/delta-docker

# Switch to root to set permissions if necessary
#USER root

WORKDIR /app
COPY . /app

# Ensure the entrypoint script is executable
#RUN chmod +x /app/entrypoint.sh

# Establish entrypoint
ENTRYPOINT ["bash", "startup.sh"]