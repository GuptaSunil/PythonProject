# Adjust this path to where you extracted Kafka
$KafkaHome = "C:\kafka"

# Change directory to Kafka installation
Set-Location $KafkaHome

# Ensure log directory exists and is clean
$logDir = "C:\tmp\kafka-logs"
if (!(Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
} else {
    Remove-Item "$logDir\*" -Recurse -Force
}

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

# Create topics for ETL pipeline
#Write-Host "Creating Kafka topics..."
#& "$KafkaHome\bin\windows\kafka-topics.bat" --create --topic raw_mssql_data_PINCODE --bootstrap-server 127.0.0.1:9092
#& "$KafkaHome\bin\windows\kafka-topics.bat" --create --topic transformed_data_PINCODE --bootstrap-server 127.0.0.1:9092

#& "$KafkaHome\bin\windows\kafka-topics.bat" --create --topic raw_mssql_data_STATE_MASTER --bootstrap-server 127.0.0.1:9092
#& "$KafkaHome\bin\windows\kafka-topics.bat" --create --topic transformed_data_STATE_MASTER --bootstrap-server 127.0.0.1:9092

#Write-Host "All topics created successfully!"
#Write-Host "Kafka broker is running at 127.0.0.1:9092"
