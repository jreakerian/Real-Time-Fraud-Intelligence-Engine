FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /opt/project


COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your producer code
COPY . .

# The command to run when the container starts
CMD ["python", "producer.py"]