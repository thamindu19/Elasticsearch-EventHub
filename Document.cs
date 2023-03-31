namespace Elasticsearch;

public class Document
{
    // public Dictionary<string, string> header { get; set; }
    public string file_id { get; set; }
    public string file_name { get; set; }
    public string message { get; set; }
    
    /*
     * {"MessageHeader": ["key1":"value1"],
     * "MessageBody": "body"}
     */
}