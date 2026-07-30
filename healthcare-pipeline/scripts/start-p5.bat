@echo off
cd /d "C:\Bi g Data\Kafka"
docker compose -f docker-compose.yml -f docker-compose.p5.yml up -d
docker compose -f docker-compose.yml -f docker-compose.p5.yml ps
echo.
echo Schema Registry: http://localhost:8083
echo Kafka UI:        http://localhost:8088
