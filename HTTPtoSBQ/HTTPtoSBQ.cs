using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;


namespace HTTPtoSBQ
{
    public class Attributes
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
    }
    public static class HTTPtoSBQ
    {
        [FunctionName("HTTPtoSBQfunction")]

        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "endpoint")] HttpRequest req,
            [ServiceBus("%serviceBusQueue%", Connection = "serviceBusConStr", EntityType = EntityType.Queue)] IAsyncCollector<string> telemetryOut,
            ILogger log, ExecutionContext context)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var iotHubHost = config["iotHubHost"];



            string IotHubConStr = config["iotHubConStr"];

            string json = await new StreamReader(req.Body).ReadToEndAsync();

            var dmessage = JsonConvert.DeserializeObject<RootObject[]>(json); //Deseriaize array using RootObject class - single array expected
            string ack = dmessage[0].ackId; //Extract ack ID
            string evntId = dmessage[0].message.attributes.eventID; //Extract event ID
            string device = dmessage[0].message.attributes.id;  // Extract device id from array
            string dataConvert = dmessage[0].message.data; // Extract base64 data from array
            var base64bytes = Convert.FromBase64String(dataConvert); //Convert Base64 to bytes
            dataConvert = Encoding.UTF8.GetString(base64bytes); // Convert bytes to UTF8 string
            string msgId = dmessage[0].message.messageId;  //Extract message id from array
            DateTime pubTime = dmessage[0].message.publishTime; //Extract publish time from array


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

            await telemetryOut.AddAsync(messageJSON);

            log.LogInformation($"**Message Sent to SBQ**:{messageJSON}");

            await telemetryOut.FlushAsync();

            return (ActionResult)new OkResult();

        }

    }

}
