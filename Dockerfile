# Use the official Microsoft Playwright image as base. 
FROM mcr.microsoft.com/playwright:v1.42.0-jammy

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    libmariadb-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY src/requirements.txt .

# 1. On corrige uniquement le bug scikit-learn
RUN sed -i 's/scikit-learn==1.4.1/scikit-learn>=1.4.1/g' requirements.txt
# 2. On enlève les numéros de version figés pour les paquets en conflit 
# (uv se chargera de trouver les versions parfaites tout seul)
RUN sed -i 's/kfp==2.16.0/kfp/g' requirements.txt
RUN sed -i 's/kfp-kubernetes==2.16.0/kfp-kubernetes/g' requirements.txt
RUN sed -i 's/google-generativeai==0.8.6/google-generativeai/g' requirements.txt

# ✨ INSTALLATION DE UV ✨
RUN pip3 install --no-cache-dir uv

# On utilise 'uv pip install' au lieu de 'pip3 install'
# L'option --system dit à uv d'installer les paquets globalement dans le conteneur
RUN uv pip install --system --no-cache-dir -r requirements.txt

# Browsers
RUN playwright install chromium

# Copy the source code
COPY src /app/src

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src:/app/src/agents
ENV DATA_DIR=/app/data

# Entrypoint
ENTRYPOINT ["python3", "-u"]