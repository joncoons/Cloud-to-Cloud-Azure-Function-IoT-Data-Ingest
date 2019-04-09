# Cloud-to-Cloud-Azure-Function-IoT-Data-Ingest


This project uses four separate Azure Functions to ingest C2C IoT data. Data is ingested through a secure HTTPS REST API (HTTP trigger), which deserializes, transforms and reserializes the data, sending it to an Azure Service Bus Queue. This queue data is consumed by another function (Service Bus Queue trigger), which extracts the device ID from the message, runs a SQL query against Cosmos DB, and uses the returned credentials to connect to IoT Hub.

A third function (Timer trigger) executes every two minutes, sending the export command to the IoT Hub device registry, which creates a new blob in Azure Storage. The fourth and final function (Blob trigger) parses this JSON text line-by-line and makes an upsert entry into a Cosmos DB collection which is used to establish the connection with IoT Hub.

Please note: This code was created for a POC and not a production environment. There are many additional considerations to take into account for production, which this code implicitly does not. In any case, I hope you find this helpful as you pursue your IoT initiative.
