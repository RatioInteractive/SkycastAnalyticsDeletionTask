//----------------------------------------------------------------------------------
// Microsoft Developer & Platform Evangelism
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
//----------------------------------------------------------------------------------
// The example companies, organizations, products, domain names,
// e-mail addresses, logos, people, places, and events depicted
// herein are fictitious.  No association with any real company,
// organization, product, domain name, email address, logo, person,
// places, or events is intended or should be inferred.
//----------------------------------------------------------------------------------

namespace DataBlobStorageSample
{
    using Microsoft.Azure;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    
    /// <summary>
    /// Azure Storage Blob Sample - Demonstrate how to use the Blob Storage service. 
    /// Blob storage stores unstructured data such as text, binary data, documents or media files. 
    /// Blobs can be accessed from anywhere in the world via HTTP or HTTPS.
    ///
    /// Note: This sample uses the .NET 4.5 asynchronous programming model to demonstrate how to call the Storage Service using the 
    /// storage client libraries asynchronous API's. When used in real applications this approach enables you to improve the 
    /// responsiveness of your application. Calls to the storage service are prefixed by the await keyword. 
    /// 
    /// Documentation References: 
    /// - What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
    /// - Getting Started with Blobs - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-blobs/
    /// - Blob Service Concepts - http://msdn.microsoft.com/en-us/library/dd179376.aspx 
    /// - Blob Service REST API - http://msdn.microsoft.com/en-us/library/dd135733.aspx
    /// - Blob Service C# API - http://go.microsoft.com/fwlink/?LinkID=398944
    /// - Delegating Access with Shared Access Signatures - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-shared-access-signature-part-1/
    /// - Storage Emulator - http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx
    /// - Asynchronous Programming with Async and Await  - http://msdn.microsoft.com/en-us/library/hh191443.aspx
    /// </summary>
    public class Program
    {
        // *************************************************************************************************************************
        // Instructions: This sample can be run using either the Azure Storage Emulator that installs as part of this SDK - or by
        // updating the App.Config file with your AccountName and Key. 
        // 
        // To run the sample using the Storage Emulator (default option)
        //      1. Start the Azure Storage Emulator (once only) by pressing the Start button or the Windows key and searching for it
        //         by typing "Azure Storage Emulator". Select it from the list of applications to start it.
        //      2. Set breakpoints and run the project using F10. 
        // 
        // To run the sample using the Storage Service
        //      1. Open the app.config file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and
        //         uncomment the connection string for the storage service (AccountName=[]...)
        //      2. Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in 
        //         the App.Config file. See http://go.microsoft.com/fwlink/?LinkId=325277 for more information
        //      3. Set breakpoints and run the project using F10. 
        // 
        // *************************************************************************************************************************
        static void Main(string[] args)
        {
            // Block blob basics
            Console.WriteLine("Skycast Analytics Deletion Task Runner \n");
            SkycastAnalyticsDeletionTaskAsync().Wait();
        }

        /// <summary>
        /// Task for deleting outdated analytics blobs that have reached older than 90 days
        /// </summary>
        /// <returns>Task<returns>
        private static async Task SkycastAnalyticsDeletionTaskAsync()
        {
            DateTime startTime = DateTime.UtcNow;
            // Retrieve storage account information from connection string
            CloudStorageAccount storageAccount = CreateStorageAccountFromConnectionString(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create a blob client for interacting with the blob service.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Create a container for organizing blobs within the storage account.
            Console.WriteLine("1. Connect to Container\n ------------------------------");
            //CloudBlobContainer container = blobClient.GetContainerReference("skycast-ife-analytics-device-outputs");
            CloudBlobContainer container = blobClient.GetContainerReference("skycast-ife-analytics-device-outputs");

            // List all blobs in this container. Because a container can contain a large number of blobs the results 
            // are returned in segments (pages) with a maximum of 5000 blobs per segment.
            Console.WriteLine("2. Delete old Blobs in Container\n ------------------------------");
            BlobContinuationToken token = null;
            int totalCount = 0;
            int deleteCount = 0;
            do
            {
                BlobResultSegment resultSegment = await container.ListBlobsSegmentedAsync(token);
                token = resultSegment.ContinuationToken;

                DateTime currentDate = DateTime.UtcNow;
                DateTime maxLastModified = currentDate.AddDays(-90);
                long maxLastModifiedTicks = maxLastModified.Ticks;

                //Uncomment for copying blobs to another container for testing
                /**
                foreach (CloudBlockBlob blob in resultSegment.Results)
                {
                    String downloadFileName = string.Format("./{0}", blob.Name);
                    await blob.DownloadToFileAsync(downloadFileName, FileMode.Create);
                    CloudBlockBlob blockBlob = testContainer.GetBlockBlobReference(blob.Name);
                    await blockBlob.UploadFromFileAsync(downloadFileName);

                }**/

                IEnumerable<IListBlobItem> toBeDeleted = resultSegment.Results.Where((b) => ((CloudBlockBlob)b).Properties.LastModified.Value.UtcTicks < maxLastModifiedTicks);
                Console.WriteLine("Total - {0} - vs To Be Deleted - {1}", resultSegment.Results.Count(), toBeDeleted.Count());
                foreach (CloudBlockBlob blob in toBeDeleted)
                {
                    await blob.DeleteAsync();
                    Console.WriteLine(" -   DELETED - {0} - LastModified: {1}", blob.Name, blob.Properties.LastModified.ToString());
                }


                totalCount = totalCount + resultSegment.Results.Count();
                deleteCount = deleteCount + toBeDeleted.Count();
            } while (token != null);

            // Clean up after the demo
            DateTime endTime = DateTime.UtcNow;
            Console.WriteLine("\n\n------------------------------ SUMMARY ------------------------------");
            Console.WriteLine("DELETION COMPLETE - Total Deleted: {0}", deleteCount);
            Console.WriteLine("DELETION START TIME: {0}", startTime.ToLongTimeString());
            Console.WriteLine("DELETION END TIME: {0}", endTime.ToLongTimeString());
            Console.WriteLine("Total Number of Blobs before Deletion: {0}", totalCount);
            Console.WriteLine("Total Number of Blobs after Deletion: {0}", totalCount - deleteCount);
            Console.WriteLine("Total Time Elapsed - {0}", TimeSpan.FromTicks(endTime.Ticks - startTime.Ticks));
        }

        /// <summary>
        /// Validates the connection string information in app.config and throws an exception if it looks like 
        /// the user hasn't updated this to valid values. 
        /// </summary>
        /// <param name="storageConnectionString">The storage connection string</param>
        /// <returns>CloudStorageAccount object</returns>
        private static CloudStorageAccount CreateStorageAccountFromConnectionString(string storageConnectionString)
        {
            CloudStorageAccount storageAccount;
            try
            {
                storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            }
            catch (FormatException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
                throw;
            }
            catch (ArgumentException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
                throw;
            }

            return storageAccount;
        }

    }
}
