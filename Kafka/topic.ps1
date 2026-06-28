# Adjust this path to where you extracted Kafka
$KafkaHome = "C:\kafka"

# Function to create topic only if it doesn't exist
function Ensure-Topic {
    param (
        [string]$TopicName
    )
    $topics = & "$KafkaHome\bin\windows\kafka-topics.bat" --list --bootstrap-server 127.0.0.1:9092
    if ($topics -notmatch $TopicName) {
        Write-Host "Creating topic: $TopicName"
        & "$KafkaHome\bin\windows\kafka-topics.bat" --create --topic $TopicName --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1
    } else {
        Write-Host "Topic '$TopicName' already exists, skipping..."
    }
}

# Create topics for ETL pipeline (only if missing)
Ensure-Topic "raw_mssql_data_PINCODE"
Ensure-Topic "transformed_data_PINCODE"
Ensure-Topic "raw_mssql_data_STATE_MASTER"
Ensure-Topic "transformed_data_STATE_MASTER"
Ensure-Topic "raw_mssql_data_LOGIN_HISTORY"
Ensure-Topic "transformed_data_LOGIN_HISTORY"

Write-Host "LoginHistory topics setup complete!"
