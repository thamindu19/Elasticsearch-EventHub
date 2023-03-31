
using Elasticsearch.Net;
using Microsoft.AspNetCore.Mvc;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

namespace Elasticsearch.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ElasticsearchController : ControllerBase
    {
        private readonly ElasticClient _elasticClient;

        public ElasticsearchController()
        {
            var connectionSettings = new ConnectionSettings(new Uri("https://4.193.154.104:9200/"))
                .DefaultIndex(".ds-logs-azure.eventhub-event_hub-2023.03.29-000001")
                .DisableDirectStreaming()
                .ServerCertificateValidationCallback(OnCertificateValidation)
                .BasicAuthentication("elastic", "=OAxbvmXbY0oRydGrYWG");
            _elasticClient = new ElasticClient(connectionSettings);
        }
        
        // Disable SSL/TLS certificate validation
        private static bool OnCertificateValidation(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        [HttpPost("Search")]
        public async Task<IActionResult> Search([FromBody] SearchRequest searchRequest)
        {
            // if (string.IsNullOrEmpty(searchRequest.Query))
            // {
            //     return BadRequest("Query parameter is required.");
            // }
            
            var searchResponse = await _elasticClient.SearchAsync<Document>(s => s
                .From(0)
                .Size(100)
                .Query(q => q
                    .Match(m => m
                        .Field(f => f.message)
                        .Query(searchRequest.query)
                    )
                )
                .Source(src => src
                    .Includes(i => i
                        .Fields(
                            f => f.file_id,
                            f => f.file_name,
                            f => f.message
                        )
                    )
                )
            );
            
            Console.WriteLine(searchResponse);
            
            var documents = searchResponse.Documents;
            return Ok(documents);
        }

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
    }

    public class SearchRequest
    {
        public string query { get; set; }
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