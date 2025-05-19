# Use a base image with Python and Java pre-installed
FROM openjdk:11-slim

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip && \
    apt-get clean

# Set Python3 as default
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install required Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code into the container
COPY . /app
WORKDIR /app

# Command to run your script
CMD ["python", "-u", "cloud_metro3.py"]
