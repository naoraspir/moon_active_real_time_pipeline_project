FROM python:3.10.0-slim

# Make working directories
RUN  mkdir -p  /moon_predictor
WORKDIR  /moon_predictor

# Upgrade pip with no cache
RUN pip install --no-cache-dir -U pip

# Copy application requirements file to the created working directory
COPY requirements.txt .

# Install application dependencies from the requirements file
RUN pip install -r requirements.txt

# Copy every file in the source folder to the created working directory
COPY  . .

# Run tests
RUN pytest --cov

# Run the python application
CMD ["python", "main.py"]