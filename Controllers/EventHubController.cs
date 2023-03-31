using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Mvc;
using System.Text;

namespace Elasticsearch.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class EventHubController : ControllerBase
    {
        private const string ConnectionString = "Endpoint=sb://elasticsearch001.servicebus.windows.net/;SharedAccessKeyName=es;SharedAccessKey=tT1Jc2EumZuy9eeNQwYVLhFWHIb4rWawH+AEhPrS7ZY=;EntityPath=elasticsearch";
        private const string EventHubName = "elasticsearch";

        private const string BlobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=es001;AccountKey=ouz2feuhCKr/XSPMzgA6ADIwoxwmr5+sGWyJ6fYiS0E34U/R0zs1eM4sPtMj0CzJL0Q8aOEdTUBe+ASttcyCUA==;EndpointSuffix=core.windows.net";
        private const string BlobContainerName = "checkpoints";

        private readonly EventHubProducerClient _producerClient;
        private readonly EventProcessorClient _processor;

        public EventHubController()
        {
            // Create a producer client to send events to an event hub
            _producerClient = new EventHubProducerClient(ConnectionString, EventHubName);

            // Create a blob container client that the event processor will use 
            BlobContainerClient storageClient = new BlobContainerClient(BlobStorageConnectionString, BlobContainerName);

            // Create an event processor client to process events in the event hub
            // _processor = new EventProcessorClient(storageClient, EventHubConsumerClient.DefaultConsumerGroupName, ConnectionString, EventHubName);
            _processor = new EventProcessorClient(storageClient, "event_receiver_api", ConnectionString, EventHubName);
        }

        [HttpPost("PostEvent")]
        public async Task<IActionResult> PostEvent([FromBody] string eventData)
        {
            // Create a batch of events 
            using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();

            // Add events to the batch. An event is a represented by a collection of bytes and metadata. 
            // eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes("First event")));
            // eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes("Second event")));
            // eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes("Third event")));
            eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(eventData)));

            // Use the producer client to send the batch of events to the event hub
            await _producerClient.SendAsync(eventBatch);
            Console.WriteLine("A batch of 4 events has been published.");

            return Ok();
        }

        [HttpGet("GetEvents")]
        public async Task GetEvents()
        {
            // Register handlers for processing events and handling errors
            _processor.ProcessEventAsync += ProcessEventHandler;
            _processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await _processor.StartProcessingAsync();

            // Wait for 10 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(10));

            // Stop the processing
            await _processor.StopProcessingAsync();
        }
        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
