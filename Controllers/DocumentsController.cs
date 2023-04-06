using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Mvc;
using iTextSharp.text.pdf.parser;
using iTextSharp.text.pdf;
using System.Text;
using Tesseract;
using Ghostscript.NET;
using Ghostscript.NET.Rasterizer;
using System.Drawing.Imaging;
using Azure.Storage.Blobs.Models;
using Newtonsoft.Json;

namespace Elasticsearch.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DocumentsController : ControllerBase
    {
        private const string ConnectionString =
            "Endpoint=sb://elasticsearch001.servicebus.windows.net/;SharedAccessKeyName=es;SharedAccessKey=tT1Jc2EumZuy9eeNQwYVLhFWHIb4rWawH+AEhPrS7ZY=;EntityPath=elasticsearch";

        private const string EventHubName = "elasticsearch";

        private const string BlobStorageConnectionString =
            "DefaultEndpointsProtocol=https;AccountName=es001;AccountKey=ouz2feuhCKr/XSPMzgA6ADIwoxwmr5+sGWyJ6fYiS0E34U/R0zs1eM4sPtMj0CzJL0Q8aOEdTUBe+ASttcyCUA==;EndpointSuffix=core.windows.net";

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

        [HttpPost("PostPdf")]
        public async Task<IActionResult> PostPdf([FromForm(Name = "pdfFile")] IFormFile pdfFile,
            [FromForm(Name = "tags")] string[] tags, [FromForm(Name = "accessRoles")] string[] accessRoles)
        {
            // Generate ID for PDF file
            var fileId = Guid.NewGuid();

            // Upload the PDF file to Azure Blob Storage
            var blobName = $"{fileId}.pdf";

            // Get a reference to a block blob in the container
            BlobClient blobClient = _storageClient.GetBlobClient(blobName);

            // Open a FileStream to the file you want to upload
            using (Stream stream = pdfFile.OpenReadStream())
            {
                // Upload the file to the blob
                await blobClient.UploadAsync(stream, true);
            }

            // Add the tags to the blob metadata
            IDictionary<string, string> metadata = new Dictionary<string, string>();
            if (tags != null) 
            {
                metadata.Add("file_name", pdfFile.FileName);
                metadata.Add("tags", String.Join(',', tags));
            }

            await blobClient.SetMetadataAsync(metadata);

            // Create a Ghostscript rasterizer instance
            using (var rasterizer = new GhostscriptRasterizer())
            {
                //  Create an instance of Tesseract OCR engine
                using (var engine = new TesseractEngine("C:/Program Files/Tesseract-OCR/tessdata", "eng",
                           EngineMode.Default))
                {
                    // Initialize the rasterizer with the PDF file
                    rasterizer.Open(pdfFile.OpenReadStream());

                    string text = "";

                    // Loop through each page of the PDF
                    for (int i = 1; i <= rasterizer.PageCount; i++)
                    {
                        // Convert the current page to an image
                        using (var pageImage = rasterizer.GetPage(300, i))
                        {
                            // Save the image to a MemoryStream
                            using (var memoryStream = new MemoryStream())
                            {
                                pageImage.Save(memoryStream, System.Drawing.Imaging.ImageFormat.Png);
                                // Reset the MemoryStream position
                                memoryStream.Position = 0;

                                // Perform OCR on the image using Tesseract
                                using (var page = engine.Process(Pix.LoadFromMemory(memoryStream.ToArray())))
                                {
                                    // Extract the text from the page
                                    text += page.GetText();
                                }
                            }
                        }
                    }

                    // Create a message containing the PDF text and metadata
                    var document = new Document
                    {
                        // header = new Dictionary<string, string>
                        // {
                        //     ["file_id"] = $"{fileId}",
                        //     ["file_name"] = pdfFile.FileName,
                        //     ["tags"] = tags
                        // },
                        file_id = $"{fileId}",
                        file_name = pdfFile.FileName,
                        tags = tags[0].Split(','),
                        message = text,
                        access_roles = accessRoles[0].Split(',')
                    };

                    // Create a batch of events
                    using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();

                    // Serialize the message to JSON and create an EventData object
                    var eventData = new EventData(JsonConvert.SerializeObject(document));
                    // var eventData = new EventData(text);

                    // Add the EventData object to the batch
                    eventBatch.TryAdd(eventData);

                    // Send the batch of events to Event Hub
                    await _producerClient.SendAsync(eventBatch);
                }
            }

            return Ok();
        }

        [HttpGet("GetPdf")]
        public async Task<FileStreamResult> GetPdf(string fileId)
        {
            // Get a reference to the block blob in the container
            BlobClient blobClient = _storageClient.GetBlobClient($"{fileId}.pdf");

            // Get the PDF file from the blob
            var stream = await blobClient.OpenReadAsync();

            // Return the file as a FileStreamResult
            return new FileStreamResult(stream, "application/pdf")
            {
                FileDownloadName = $"{fileId}.pdf"
            };
        }
    }
}

//
// // Get the PDF file from the blob
// var stream = await blobClient.OpenReadAsync();
//
// // Return the file as a FileStreamResult
// return new FileStreamResult(stream, "application/pdf")
// {
//     FileDownloadName = $"{fileId}.pdf"
// };


// [HttpPost("PostPdf")]
        // public async Task<IActionResult> PostPdf([FromForm(Name = "pdfFile")] IFormFile pdfFile, [FromForm(Name = "tags")] string tags)
        // {
        //     // Generate ID for PDF file
        //     var fileId = Guid.NewGuid();
        //
        //     // Upload the PDF file to Azure Blob Storage
        //     var blobName = $"{fileId}.pdf";
        //
        //     // Get a reference to a block blob in the container
        //     BlobClient blobClient = _storageClient.GetBlobClient(blobName);
        //
        //     // Open a FileStream to the file you want to upload
        //     using (Stream stream = pdfFile.OpenReadStream())
        //     {
        //         // Upload the file to the blob
        //         await blobClient.UploadAsync(stream, true);
        //     }
        //     
        //     // Add the tags to the blob metadata
        //     IDictionary<string, string> metadata = new Dictionary<string, string>();
        //     if (!string.IsNullOrEmpty(tags))
        //     {
        //         metadata.Add("tags", tags);
        //     }
        //     await blobClient.SetMetadataAsync(metadata);
        //     
        //     // Get the PDF file from the blob
        //     using (var stream = new MemoryStream())
        //     {
        //         await blobClient.DownloadToAsync(stream);
        //         
        //             // Create a Ghostscript rasterizer instance
        //             using (var rasterizer = new GhostscriptRasterizer())
        //             {
        //                 //  Create an instance of Tesseract OCR engine
        //                 using (var engine = new TesseractEngine("C:/Program Files/Tesseract-OCR/tessdata", "eng",
        //                            EngineMode.Default))
        //                 {
        //                     // Initialize the rasterizer with the PDF file
        //                     rasterizer.Open(stream);
        //                     
        //                     string text = "";
        //                     
        //                     // Loop through each page of the PDF
        //                     for (int i = 1; i <= rasterizer.PageCount; i++)
        //                     {
        //                         // Convert the current page to an image
        //                         using (var pageImage = rasterizer.GetPage(300, i))
        //                         {
        //                             // Save the image to a MemoryStream
        //                             using (var memoryStream = new MemoryStream())
        //                             {
        //                                 pageImage.Save(memoryStream, System.Drawing.Imaging.ImageFormat.Png);
        //                                 // Reset the MemoryStream position
        //                                 memoryStream.Position = 0;
        //
        //                                 // Perform OCR on the image using Tesseract
        //                                 using (var page = engine.Process(Pix.LoadFromMemory(memoryStream.ToArray())))
        //                                 {
        //                                     // Extract the text from the page
        //                                     text += page.GetText();
        //                                     
        //                                     // Console.WriteLine(page.GetText());
        //                                 }
        //                             }
        //                         }
        //                     }
        //                     
        //                     // // Get the blob properties, including the metadata
        //                     // BlobProperties properties = await blobClient.GetPropertiesAsync();
        //                     //
        //                     // // Get the metadata dictionary from the properties
        //                     // IDictionary<string, string> metadata = properties.Metadata;
        //                     //
        //                     // // Access individual metadata values by key
        //                     // if (metadata.TryGetValue("tags", out string tags))
        //                     // {
        //                     //     // 
        //                     // }
        //                     //
        //                     // Create a message containing the PDF text and metadata
        //                     var message = new CustomMessage
        //                     {
        //                         header = new Dictionary<string, string>
        //                         {
        //                             ["file_id"] = $"{fileId}",
        //                             ["file_name"] = pdfFile.FileName,
        //                             ["tags"] = tags
        //                         },
        //                         message = text
        //                     };
        //                     
        //                     // Create a batch of events
        //                     using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();
        //
        //                     // Serialize the message to JSON and create an EventData object
        //                     var eventData = new EventData(JsonConvert.SerializeObject(message));
        //                     // var eventData = new EventData(message.ToString());
        //
        //                     // Send the text to the Event Hub
        //                     // var eventData = new EventData(Encoding.UTF8.GetBytes(text));
        //                     // eventData.Properties.Add("filename", blobName);
        //                     // eventBatch.TryAdd(eventData);
        //                     // await _producerClient.SendAsync(eventBatch);
        //
        //                     // Add the EventData object to the batch
        //                     eventBatch.TryAdd(eventData);
        //                     
        //                     // Send the batch of events to Event Hub
        //                     await _producerClient.SendAsync(eventBatch);
        //                 }

                        
                        
            // // Get the PDF file from the blob
            // using (var stream = new MemoryStream())
            // {
            //     await blobClient.DownloadToAsync(stream);
            //     
            //         // Create a Ghostscript rasterizer instance
            //         using (var rasterizer = new GhostscriptRasterizer())
            //         {
            //             //  Create an instance of Tesseract OCR engine
            //             using (var engine = new TesseractEngine("C:/Program Files/Tesseract-OCR/tessdata", "eng",
            //                        EngineMode.Default))
            //             {
            //                 // Initialize the rasterizer with the PDF file
            //                 rasterizer.Open(stream);
            //
            //                 string text = "";
            //                 // Loop through each page of the PDF
            //                 for (int i = 1; i <= rasterizer.PageCount; i++)
            //                 {
            //                     // Convert the current page to an image
            //                     using (var pageImage = rasterizer.GetPage(300, i))
            //                     {
            //                         // Save the image to a MemoryStream
            //                         using (var memoryStream = new MemoryStream())
            //                         {
            //                             pageImage.Save(memoryStream, System.Drawing.Imaging.ImageFormat.Png);
            //                             // Reset the MemoryStream position
            //                             memoryStream.Position = 0;
            //
            //                             // Perform OCR on the image using Tesseract
            //                             using (var page = engine.Process(Pix.LoadFromMemory(memoryStream.ToArray())))
            //                             {
            //                                 // Print the extracted text to the console
            //                                 Console.WriteLine(page.GetText());
            //
            //                                 text += page.GetText();
            //
            //                                 var message = new CustomMessage
            //                                 {
            //                                     MessageHeader = new Dictionary<string, string>
            //                                     {
            //                                         ["filename"] = blobName
            //                                     },
            //                                     MessageBody = text
            //                                 };
            //
            //                                 // Create a batch of events
            //                                 using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();
            //
            //                                 // Send the text to the Event Hub
            //                                 // var eventData = new EventData(Encoding.UTF8.GetBytes(text));
            //                                 // eventData.Properties.Add("filename", blobName);
            //                                 // eventBatch.TryAdd(eventData);
            //                                 var eventData = new EventData(JsonConvert.SerializeObject(message));
            //                                 eventBatch.TryAdd(eventData);
            //                                 await _producerClient.SendAsync(eventBatch);
            //                             }
            //                         }
            //                     }
            //                 }
            //             
            //         }


                    // // Get the PDF file from the blob
                    // using (var fileStream = new FileStream(blobClient.Uri.AbsoluteUri, FileMode.Open))
                    // {
                    //     // Create an instance of Tesseract OCR engine
                    //     using (var engine = new TesseractEngine("C:/Program Files/Tesseract-OCR/tessdata", "eng", EngineMode.Default))
                    //     {
                    //         // Create a Ghostscript rasterizer instance
                    //         using (var rasterizer = new GhostscriptRasterizer())
                    //         {
                    //             // Initialize the rasterizer with the PDF file
                    //             rasterizer.Open(fileStream);
                    //
                    //             // Loop through each page of the PDF
                    //             for (int i = 1; i <= rasterizer.PageCount; i++)
                    //             {
                    //                 // Convert the current page to an image
                    //                 using (var pageImage = rasterizer.GetPage(300, i))
                    //                 {
                    //                     // Save the image to a MemoryStream
                    //                     using (var memoryStream = new MemoryStream())
                    //                     {
                    //                         pageImage.Save(memoryStream, System.Drawing.Imaging.ImageFormat.Png);
                    //                         // Reset the MemoryStream position
                    //                         memoryStream.Position = 0;
                    //
                    //                         // Perform OCR on the image using Tesseract
                    //                         using (var page = engine.Process(Pix.LoadFromMemory(memoryStream.ToArray())))
                    //                         {
                    //                             // Print the extracted text to the console
                    //                             Console.WriteLine(page.GetText());
                    //
                    //                             var text = page.GetText();
                    //
                    //                             // Create a batch of events
                    //                             using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();
                    //
                    //                             // Send the text to the Event Hub
                    //                             eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(text)));
                    //                             await _producerClient.SendAsync(eventBatch);
                    //                         }
                    //                     }
                    //                 }
                    //             }
                    //         }
                    //     }

                    // // Extract text from the PDF file
                    // using (var pdfReader = new PdfReader(blobClient.Uri.AbsoluteUri))
                    // {
                    //     
                    //     // Create an instance of Tesseract OCR engine
                    //     using (var engine = new TesseractEngine("C:/Tesseract-OCR/tessdata", "eng", EngineMode.Default)) {
                    //         // Loop through each page of the PDF
                    //         for (int i = 1; i <= pdfReader.NumberOfPages; i++) {
                    //             // Extract the text from the current page using OCR
                    //             var strategy = new SimpleTextExtractionStrategy();
                    //             var currentPageText = PdfTextExtractor.GetTextFromPage(pdfReader, i, strategy);
                    //             var page = engine.Process(Pix.LoadFromMemory(Encoding.UTF8.GetBytes(currentPageText)));
                    //     
                    //             // Print the extracted text to the console
                    //             Console.WriteLine(page.GetText());
                    //     
                    //             var text = page.GetText();
                    //             // Create a batch of events 
                    //             using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();
                    //     
                    //             // Send the text to the Event Hub 
                    //             eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(text)));
                    //             await _producerClient.SendAsync(eventBatch);
                    //         }
                    //     }

                    // var text = PdfTextExtractor.GetTextFromPage(pdfReader, 1);
                    //
                    // // Create a batch of events 
                    // using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();
                    //
                    // // Send the text to the Event Hub 
                    // eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(text)));
                    // await _producerClient.SendAsync(eventBatch);

                    // Index the text in Elastic search
                    // var indexResponse = await _client.IndexAsync(new { content = text });
                    // if (!indexResponse.IsValid)
                    // {
                    //     return BadRequest("Failed to index PDF text in Elastic search");
                    // }
