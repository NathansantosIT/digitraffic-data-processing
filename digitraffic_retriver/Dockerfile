FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file first for caching purposes
COPY requirements.txt .

# Update pip and install dependencies from requirements.txt
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Specify the command to run the API retriever script
CMD ["python", "-u", "scr/api_retriever.py"]