# Adjust this path to where you extracted Kafka
$KafkaHome = "C:\kafka"

# Change directory to Kafka installation
Set-Location $KafkaHome

# Start Zookeeper in a new PowerShell window
Write-Host "Starting Zookeeper..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$KafkaHome\bin\windows\zookeeper-server-start.bat $KafkaHome\config\zookeeper.properties" -WindowStyle Normal

# Wait a few seconds for Zookeeper to initialize
Start-Sleep -Seconds 10

# Start Kafka broker in a new PowerShell window
Write-Host "Starting Kafka broker..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$KafkaHome\bin\windows\kafka-server-start.bat $KafkaHome\config\server.properties" -WindowStyle Normal

# Wait for Kafka to come up
Start-Sleep -Seconds 10

# Test connectivity
Write-Host "Testing Kafka broker connectivity..."
Test-NetConnection -ComputerName 127.0.0.1 -Port 9092
