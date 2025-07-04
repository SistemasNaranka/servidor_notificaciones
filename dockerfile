# Usa Python 3.12 oficial
FROM python:3.12-slim

# Establece directorio de trabajo
WORKDIR /app

# Copia e instala dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script
COPY notification_server.py .

# Expone el puerto 5050
EXPOSE 5050

# Lanza el servidor
CMD ["uvicorn", "notification_server:app", "--host", "0.0.0.0", "--port", "8000"]
