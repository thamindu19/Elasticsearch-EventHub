
using Elasticsearch.Net;
using Microsoft.AspNetCore.Mvc;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Azure.Core;
using Newtonsoft.Json;

namespace Elasticsearch.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ElasticsearchController : ControllerBase
    {
        private readonly ElasticClient _elasticClient;
        private readonly ILogger<ElasticsearchController> _logger;

        public ElasticsearchController(ILogger<ElasticsearchController> logger)
        {
            var connectionSettings = new ConnectionSettings(new Uri("https://4.193.154.104:9200/"))
                .DefaultIndex(".ds-logs-azure.eventhub-event_hub-2023.03.29-000001")
                .DisableDirectStreaming()
                .ServerCertificateValidationCallback(OnCertificateValidation)
                .BasicAuthentication("elastic", "=OAxbvmXbY0oRydGrYWG");
            _elasticClient = new ElasticClient(connectionSettings);
            _logger = logger;
        }
        
        // Disable SSL/TLS certificate validation
        private static bool OnCertificateValidation(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        [HttpPost("Search")]
        public async Task<IActionResult> Search([FromForm(Name = "message")] string message, [FromForm(Name = "tags")
        ] string[] tags, [FromForm(Name = "userRole")] string userRole)
        {
            // if (string.IsNullOrEmpty(searchRequest.Query))
            // {
            //     return BadRequest("Query parameter is required.");
            // }

            var response = await _elasticClient.SearchAsync<EsDocument>(s => s
                .From(0)
                .Size(100)
                .Query(q => q
                    .Bool(b => b
                        .Must(mu => mu
                                .Match(m => m
                                    .Field(f => f.azure.eventhub.message)
                                    .Query(message)
                                )
                            // // All users can view public documents
                            // mu => mu
                            //     .Term(t => t
                            //         .Field(f => f.azure.eventhub.access_roles)
                            //         .Contains(false)
                            //     ),
                            // Users with the "admin" role can view all documents
                            // q => userRole == "admin" && q
                            //     .MatchAll(),
                            // // Users with the "user" role can only view documents that they created
                            // q => searchRequest.userRole == "user" && q
                            //     .Term(t => t
                            //         .Field(f => f.azure.eventhub.access_roles)
                            //         .Value("username")
                            //     )
                        )
                        .Filter(fi => fi
                            .Terms(t => t
                                .Field(f => f.azure.eventhub.tags)
                                .Terms(tags)
                            )
                        )
                        .Filter(fi => fi
                            .Terms(t => t
                                .Field(f => f.azure.eventhub.access_roles)
                                .Terms(userRole)
                            )
                        )
                    )
                )
                .Source(src => src
                            .Includes(i => i
                                .Fields(
                                    f => f.azure.eventhub.file_id,
                                    f => f.azure.eventhub.file_name,
                                    f => f.azure.eventhub.tags,
                                    f => f.azure.eventhub.access_roles,
                                    f => f.azure.eventhub.message
                                )
                            )
                        )
                    );
            
            if (response.IsValid)
            {
                var documents = new List<Document>();
                foreach (var doc in response.Documents)
                {
                    var document = new Document
                    {
                        file_id = doc.azure.eventhub.file_id,
                        file_name = doc.azure.eventhub.file_name,
                        tags = doc.azure.eventhub.tags,
                        access_roles = doc.azure.eventhub.access_roles,
                        message = doc.azure.eventhub.message
                    };
                    documents.Add(document);
                }
        
                var json = JsonConvert.SerializeObject(documents, Formatting.Indented);
                _logger.LogInformation($"Found {documents.Count} documents.");
                return Ok(json);
            }
            else
            {
                _logger.LogError($"Error performing search: {response.DebugInformation}");
                return StatusCode((int)response.ApiCall.HttpStatusCode);
            }
        }
        // mu => mu
        //     .Wildcard(w => w
        //     .Field(f => f.azure.eventhub.tags)
        // .Value($"*{searchRequest.tags}*")
        // ),
        
        
        // [HttpPost("PostData")]
        // public async Task<IActionResult> PostData()
        // {
        //     var document = new Document
        //     {
        //         header = new Dictionary<string, string>
        //         {
        //             ["file_id"] = "000",
        //             ["file_name"] = "filename",
        //             ["tags"] = "tags"
        //         },
        //         message = "text"
        //     };
        //     // var indexResponse = _elasticClient.IndexDocument(document);
        //     var asyncIndexResponse = await _elasticClient.IndexDocumentAsync(document);
        //     Console.WriteLine(asyncIndexResponse);
        //     return Ok();
        // }
        
        [HttpGet("GetIndices")]
        public async Task<IActionResult> GetIndices()
        {
            // Create a request to get a list of all the indices in Elasticsearch
            var request = new CatIndicesRequest();

            // Execute the request and get the response
            var response = _elasticClient.Cat.Indices(request);

            // Iterate over the response to get the names of all the indices
            foreach (var index in response.Records)
            {
                 Console.WriteLine(index.Index);
            }

            return Ok(response.Records);
        }

        [HttpDelete("DeleteDocuments")]
        public async Task DeleteFromIndex(string index)
        {
            var deleteResponse = await _elasticClient.DeleteByQueryAsync<Document>(q => q
                .MatchAll()
                .Index(index)
            );

            Console.WriteLine(deleteResponse);
        }
    }
    
    // public class SearchRequest
    // {
    //     public string message { get; set; }
    //     public string userRole { get; set; }
    //     public string[] tags { get; set; }
    // }

    public class EsDocument
    {
        public Azure azure;
    }

    public class Azure
    {
        public EventHub eventhub;
    }
    
    public class EventHub
    {
        public string file_id { get; set; }
        public string file_name { get; set; }
        public string[] tags { get; set; }
        public string[] access_roles { get; set; }
        public string message { get; set; }
    }
    
}








// using Azure;
// using Microsoft.AspNetCore.Mvc;
// using Nest;
//
// namespace Elasticsearch.Controllers
// {
//     [ApiController]
//     [Route("[controller]")]
//     public class ElasticsearchController : ControllerBase
//     {
//         private const string ConnectionString = "https://4.193.154.104:9200/";
//
//         private readonly ElasticClient _elasticClient;
//
//         public ElasticsearchController()
//         {
//             // Create an elastic client to search from Elasticsearch
//             _elasticClient = new ElasticClient(new ConnectionSettings(new Uri(ConnectionString)).DefaultIndex(".ds-logs-azure.eventhub-event_hub-2023.03.29-000001"));
//         }
//
//         // [HttpPost("Search")]
//         // public async Task<IActionResult> Search([FromBody] string query)
//         // {
//         //     var response = await _elasticClient.SearchAsync<CustomMessage>(s => s
//         //         .Query(q => q
//         //             .Match(m => m
//         //                 .Field(f => f.message)
//         //                 .Query(query)
//         //             )
//         //         )
//         //     );
//         //     
//         //     Console.WriteLine(response);
//         //     
//         //     Console.WriteLine(response.DebugInformation);
//         //     
//         //     var results = response.Documents.ToList();
//         //
//         //     return Ok(results);
//         // }
//         
//         [HttpGet("Search")]
//         public ISearchResponse<CustomMessage> Search()
//         {
//             var results = _elasticClient.Search<CustomMessage>(s => s
//                 .Query(q => q
//                     .MatchAll()
//                 )
//             );
//             return results;
//         }
//     }
// }