# Use the official Python 3.10 image
FROM python:3.10

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/home/user \
    PATH="/home/user/.local/bin:${PATH}"

# Set the working directory
WORKDIR /app

# Create a non-root user with UID 1000
RUN useradd -m -u 1000 user

# Install system dependencies if required
# (e.g., build-essential for some python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt and install dependencies
COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY --chown=user . .

# Expose the default Hugging Face Spaces port
EXPOSE 7860

# Switch to the non-root user
USER user

# Command to run the application
CMD ["python", "app.py"]
