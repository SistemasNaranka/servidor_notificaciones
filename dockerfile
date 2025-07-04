# Uso Python 3.12 
FROM python:3.12-slim

# Establece directorio de trabajo
WORKDIR /app

# Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script
COPY notification_server.py .

# Expone el puerto 5050
EXPOSE 5050

# Lanza el servidor
CMD ["uvicorn", "notification_server:app", "--host", "0.0.0.0", "--port", "5050"]

# docker run -d -p 5050:5050
# docker run -d -p 5050:5050 --name notify notification-server
