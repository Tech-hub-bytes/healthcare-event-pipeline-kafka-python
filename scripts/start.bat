@echo off
cd /d "C:\Bi g Data\Kafka"
docker compose up -d
docker compose ps
echo.
echo Kafka broker: localhost:9092
echo Kafka UI:     http://localhost:8088
