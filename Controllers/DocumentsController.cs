using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Mvc;
using iTextSharp.text.pdf.parser;
using iTextSharp.text.pdf;
using System.Text;

namespace Elasticsearch.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DocumentsController : ControllerBase
    {
        private const string ConnectionString = "Endpoint=sb://elasticsearch1.servicebus.windows.net/;SharedAccessKeyName=SAPes;SharedAccessKey=R2hq8z6ld3vutFJz4Q0boE9IDjlQbnSXv+AEhA9oTi8=;EntityPath=elasticsearch";
        private const string EventHubName = "elasticsearch";

        private const string BlobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=elasticsearchbbd4;AccountKey=isWU55AfHxeeuq0g53et91dIgvK17Zc94GTvF7wWO8+SPiEOq9XiL+U/9ctB+ABDQc0ngjMVVn16+ASt3+dDPw==;EndpointSuffix=core.windows.net";
        private const string BlobContainerName = "pdfdocuments";

        private readonly EventHubProducerClient _producerClient;
        private readonly BlobContainerClient _storageClient;

        public DocumentsController()
        {
            // Create a producer client to send events to an event hub
            _producerClient = new EventHubProducerClient(ConnectionString, EventHubName);

            // Create a blob container client that the event processor will use 
            _storageClient = new BlobContainerClient(BlobStorageConnectionString, BlobContainerName);

        }

        [HttpPost(Name = "PostPdf")]
        public async Task<IActionResult> PostPdf([FromForm(Name = "pdfFile")] IFormFile pdfFile)
        {
            // Upload the PDF file to Azure Blob Storage
            var blobName = $"{Guid.NewGuid()}.pdf";

            // Get a reference to a block blob in the container
            BlobClient blobClient = _storageClient.GetBlobClient(blobName);

            // Open a FileStream to the file you want to upload
            using (Stream stream = pdfFile.OpenReadStream())
            {
                // Upload the file to the blob
                await blobClient.UploadAsync(stream, true);
            }

            // Extract text from the PDF file
            using (var pdfReader = new PdfReader(blobClient.Uri.AbsoluteUri))
            {
                var text = PdfTextExtractor.GetTextFromPage(pdfReader, 1);

                // Create a batch of events 
                using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();

                // Send the text to the Event Hub 
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(text)));
                await _producerClient.SendAsync(eventBatch);

                // Index the text in Elastic search
                // var indexResponse = await _client.IndexAsync(new { content = text });
                // if (!indexResponse.IsValid)
                // {
                //     return BadRequest("Failed to index PDF text in Elastic search");
                // }
            }

            return Ok();
        }

    }
}