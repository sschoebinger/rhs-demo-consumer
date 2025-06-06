FROM --platform=linux/amd64 python:3.9-slim as build

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/

# Command to run the consumer application
ENTRYPOINT ["python", "-u", "src/consumer-http.py"]