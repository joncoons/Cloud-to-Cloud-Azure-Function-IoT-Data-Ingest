using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Devices;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;


namespace IoTHubIDReplicate
{
    public class IotHubIDReplicate
    {
        [FunctionName("IotHubIDReplicate")]
        public static async void RunAsync([TimerTrigger("0 */2 * * * *")]TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var BlobConStr = config["blobConStr"];
            var BlobContainer = config["blobContainer"];
            var BlobURL = config["blobURL"];
            var IotHubConStr = config["iotHubConStr"];

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(BlobConStr);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(BlobContainer);
        
            string storedPolicyName = null;
        
            string sasContainerToken;

            if (storedPolicyName == null)
            {
                SharedAccessBlobPolicy adHocPolicy = new SharedAccessBlobPolicy()
                {
                    SharedAccessExpiryTime = DateTime.UtcNow.AddHours(1),
                    Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Delete
                };

                sasContainerToken = container.GetSharedAccessSignature(adHocPolicy, null);

                Console.WriteLine("SAS for blob container (ad hoc): {0}", sasContainerToken); 
            }
            else
            {
                sasContainerToken = container.GetSharedAccessSignature(null, storedPolicyName);

                Console.WriteLine("SAS for blob container (stored access policy): {0}", sasContainerToken);
            }

            string containerSasUri = container.Uri + sasContainerToken;

            
            Console.WriteLine("Blob SAS URI:  {0}", containerSasUri);

            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(IotHubConStr);
            JobProperties exportJob = await registryManager.ExportDevicesAsync(containerSasUri, false);
        }
        
    }

}
