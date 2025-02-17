# entrypoint.sh
#!/bin/bash
set -e

# Run your PySpark script to create the file
echo "Running PySpark script to create file..."
spark-submit /app/create_file.py

# Now start the Jupyter Notebook server
echo "Starting Jupyter Notebook..."
exec start-notebook.sh "$@"