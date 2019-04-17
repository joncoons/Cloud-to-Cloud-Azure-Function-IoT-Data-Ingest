using System;
using System.Text;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Devices.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;

namespace SBQtoIoTHub
{
    public static class SBQtoIoTHub
    {
        [FunctionName("SBQtoIoTHub")]
        public static async void Run([ServiceBusTrigger("%serviceBusQueue%", Connection = "serviceBusConStr")]string queueJson, ILogger log, ExecutionContext context)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {queueJson}");

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var dBaseUri = config["cosmosURI"];
            var dBase = config["cosmosDbName"];
            var dKey = config["cosmosPrimKey"];
            var dBaseCollection = config["cosmosCollection"];
            var iotHubHost = config["iotHubHost"];

            dynamic jsonStream = JsonConvert.DeserializeObject(queueJson);
            string device = jsonStream.id;

            var messagePayload = new Message(Encoding.UTF8.GetBytes(queueJson));

            IDocumentClient client = new DocumentClient(new Uri(dBaseUri), dKey);
            try
            {
                var collUri = UriFactory.CreateDocumentCollectionUri(dBase, dBaseCollection);
                string sql = "SELECT VALUE d.authentication.symmetricKey.primaryKey FROM " + dBaseCollection + " d WHERE d.id = \"" + device + "\"";
                Console.WriteLine(sql);
                var primKey = client.CreateDocumentQuery(collUri, sql).ToList();
                dynamic primaryKey = primKey[0];

                var iotHubConnection = "HostName=" + iotHubHost + ";DeviceId=" + device + ";SharedAccessKey=" + primaryKey + "";

                var deviceClient = DeviceClient.CreateFromConnectionString(iotHubConnection, TransportType.Amqp);
                await deviceClient.SendEventAsync(messagePayload);
            }
            finally
            {
                if (client != null)
                {
                    (client as IDisposable).Dispose();
                }
            }

        }

    }

}
