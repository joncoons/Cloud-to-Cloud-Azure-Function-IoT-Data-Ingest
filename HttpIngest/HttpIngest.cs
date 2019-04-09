using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Devices.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Documents.Client;

namespace HttpIngest
{
    /*public class Attributes
    {
        public string eventID { get; set; }
        public string id { get; set; }
    }

    public class Message
    {
        public Attributes attributes { get; set; }
        public string data { get; set; }
        public string messageId { get; set; }
        public DateTime publishTime { get; set; }
    }

    public class RootObject
    {
        public string ackId { get; set; }
        public Message message { get; set; }
    }*/
    public static class HttpIngest
    {
        [FunctionName("HttpIngest")]

        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req, ILogger log, ExecutionContext context)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

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

            

            string IotHubConStr = config["iotHubConStr"];

            string json = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic rootObject = JsonConvert.DeserializeObject(json);
            string ack = rootObject[0].ackId;
            Console.WriteLine(ack);
            //dynamic arrayObject = JsonConvert.DeserializeObject(rootObject.Filter.ToString());
            
            string evntId = rootObject[0].message.attributes.eventID; //Extract event ID
            string device = rootObject[0].message.attributes.id;  // Extract device id from array
            string dataConvert = rootObject[0].message.data; // Extract base64 data from array
            var base64bytes = Convert.FromBase64String(dataConvert);
            dataConvert = Encoding.UTF8.GetString(base64bytes);
            string msgId = rootObject[0].message.messageId;
            DateTime pubTime = rootObject[0].message.publishTime;

            /*var dmessage = JsonConvert.DeserializeObject<RootObject[]>(json);
            string ack = dmessage[0].ackId;
            string evntId = dmessage[0].message.attributes.eventID; //Extract event ID
            string device = dmessage[0].message.attributes.id;  // Extract device id from array
            string dataConvert = dmessage[0].message.data; // Extract base64 data from array
            var base64bytes = Convert.FromBase64String(dataConvert);
            dataConvert = Encoding.UTF8.GetString(base64bytes);
            string msgId = dmessage[0].message.messageId;   
            DateTime pubTime = dmessage[0].message.publishTime;
            */

            var atTelemetry = new
            {
                ackId = ack,
                eventID = evntId,
                id = device,
                data = dataConvert,
                messageId = msgId,
                publishTime = pubTime
            };

            var messageJSON = JsonConvert.SerializeObject(atTelemetry);
            var messagePayload = new Message(Encoding.ASCII.GetBytes(messageJSON));




            Console.WriteLine(device);
            Console.WriteLine(messageJSON);

            using (DocumentClient newclient = new DocumentClient(new Uri(dBaseUri), dKey))
            {
                var collUri = UriFactory.CreateDocumentCollectionUri(dBase, dBaseCollection);
                string sql = "SELECT VALUE d.authentication.symmetricKey.primaryKey FROM " + dBaseCollection + " d WHERE d.id = \"" + device + "\"";
                Console.WriteLine(sql);
                //IQueryable<string> keyList = client.CreateDocumentQuery<string>(collUri, new SqlQuerySpec(sql));
                var primKey = newclient.CreateDocumentQuery(collUri, sql).ToList();
                var primaryKey = primKey[0];
                Console.WriteLine(primaryKey);
                                              
                var iotHubConnection = "HostName=" + iotHubHost + ";DeviceId=" + device + ";SharedAccessKey=" + primaryKey + "";
                Console.WriteLine(iotHubConnection);
                

                var deviceClient = DeviceClient.CreateFromConnectionString(iotHubConnection, Microsoft.Azure.Devices.Client.TransportType.Amqp);
                await deviceClient.SendEventAsync(messagePayload);
                Console.WriteLine("IoT Hub message sent");
            }

           /* AmqpConnectionConnectionPoolSettings connectionPoolSettings = new AmqpConnectionConnectionPoolSettings();
            connectionPoolSettings.Pooling = true;
            connectionPoolSettings.MaxPoolSize = 1;
            AmqpTransportSettings transport = new AmqpTransportSettings(Microsoft.Azure.Devices.Client.TransportType.Amqp_Tcp_Only);
            transport.AmqpConnectionConnectionPoolSettings = connectionPoolSettings;
            ITransportSettings[] transportSettings = new ITransportSettings[] { transport };

            var client = DeviceClient.Create(
                connectionString.HostName,
                new DeviceAuthenticationWithRegistrySymmetricKey(ioTHubDevice.Id, ioTHubDevice.Authentication.SymmetricKey.PrimaryKey),
                transportSettings);

            return (client, DateTimeOffset.UtcNow);

            */


            return (ActionResult)new OkResult();


    


        
        }

       

    }
}
