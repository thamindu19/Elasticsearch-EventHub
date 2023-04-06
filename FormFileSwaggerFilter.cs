using System.Linq;
using System.Reflection;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace Elasticsearch
{
    public class FormFileSwaggerFilter : IOperationFilter
    {
        public void Apply(OpenApiOperation operation, OperationFilterContext context)
        {
            // Check if the operation has any parameters with the [FromForm] attribute
            var formFileParams = context.MethodInfo.GetParameters()
                .Where(p => p.ParameterType == typeof(IFormFile) && p.GetCustomAttribute<FromFormAttribute>() != null)
                .Select(p => p.Name)
                .ToList();

            if (formFileParams.Any())
            {
                // Add the multipart/form-data consumes MIME type
                var contentTypes = operation.RequestBody.Content;
                contentTypes.Clear();
                contentTypes.Add("multipart/form-data", new OpenApiMediaType
                {
                    Schema = new OpenApiSchema
                    {
                        Type = "object",
                        Properties = new Dictionary<string, OpenApiSchema>
                        {
                            [formFileParams.First()] = new OpenApiSchema
                            {
                                Type = "string",
                                Format = "binary"
                            },
                            ["tags"] = new OpenApiSchema
                            {
                                Type = "array",
                                Items = new OpenApiSchema
                                {
                                    Type = "string"
                                },
                            },
                            ["accessRoles"] = new OpenApiSchema
                            {
                                Type = "array",
                                Items = new OpenApiSchema
                                {
                                    Type = "string"
                                },
                            }
                        }
                    }
                });
            }
        }
    }
}
