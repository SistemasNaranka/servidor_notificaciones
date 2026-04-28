# Uso Python 3.12 slim para una imagen ligera y segura
FROM python:3.12-slim

# Variables de entorno para optimizar Python en Docker
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app


# Instalación de dependencias de la aplicación
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia del código fuente (se recomienda usar .dockerignore para excluir .env)
COPY . .

# Puerto de escucha del servidor
EXPOSE 5050

# Comando de arranque apuntando al nuevo punto de entrada principal
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5050"]
