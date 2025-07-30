# servidor_notificaciones
docker build -t notification-server .

docker run -d -p 5050:5050 --name notify notification-server
