using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;

namespace BlobToCosmos
{
    public class SymmetricKey
    {
        public string primaryKey { get; set; }
        public string secondaryKey { get; set; }
    }

    public class X509Thumbprint
    {
        public object primaryThumbprint { get; set; }
        public object secondaryThumbprint { get; set; }
    }

    public class Authentication
    {
        public SymmetricKey symmetricKey { get; set; }
        public X509Thumbprint x509Thumbprint { get; set; }
        public string type { get; set; }
    }

    public class Tags
    {
    }

    public class Desired
    {
    }

    public class Location
    {
        public double lon { get; set; }
        public double lat { get; set; }
    }

    public class Reported
    {
        public Location location { get; set; }
        public int dieNumber { get; set; }
    }

    public class Properties
    {
        public Desired desired { get; set; }
        public Reported reported { get; set; }
    }

    public class Capabilities
    {
        public bool iotEdge { get; set; }
    }

    public class RootObject
    {
        public string id { get; set; }
        public string eTag { get; set; }
        public string status { get; set; }
        public Authentication authentication { get; set; }
        public string twinETag { get; set; }
        public Tags tags { get; set; }
        public Properties properties { get; set; }
        public Capabilities capabilities { get; set; }
    }
    public static class BlobToCosmos
    {
        [FunctionName("BlobToCosmos")]
        public static async void Run([BlobTrigger("%blobContainer%/{name}", Connection = "blobConStr")]
            Stream blobInput, string name, ILogger log, ExecutionContext context)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {blobInput.Length} Bytes");

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var dBaseUri = config["cosmosURI"];
            var dBase = config["cosmosDbName"];
            var dKey = config["cosmosPrimKey"];
            var dBaseCollection = config["cosmosCollection"];

            StreamReader reader = new StreamReader(blobInput);

            IDocumentClient client = new DocumentClient(new Uri(dBaseUri), dKey);
            try
            {
                while (reader.Peek() != -1)
                {
                    string deviceArray = await reader.ReadLineAsync();
                    var dDeviceArray = JsonConvert.DeserializeObject<RootObject>(deviceArray);
                    string deviceID = dDeviceArray.id;
                    var jDeviceArray = JsonConvert.SerializeObject(dDeviceArray);

                    var collUri = UriFactory.CreateDocumentCollectionUri(dBase, dBaseCollection);
                    var upsertAction = await client.UpsertDocumentAsync(collUri, dDeviceArray);
                    var upsert = upsertAction.Resource;

                    //Console.WriteLine("Request charge of upsert operation: {0}", upsertAction.RequestCharge);
                    //Console.WriteLine("StatusCode: {0}", upsertAction.StatusCode);
                }
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
