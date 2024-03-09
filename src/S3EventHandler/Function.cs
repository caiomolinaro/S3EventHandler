using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace S3EventHandler;

public class Function
{
    private IAmazonS3 S3Client { get; set; }

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {
        S3Client = new AmazonS3Client();
    }

    /// <summary>
    /// Constructs an instance with a preconfigured S3 client. This can be used for testing outside of the Lambda environment.
    /// </summary>
    /// <param name="s3Client">The service client to access Amazon S3.</param>
    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;
    }

    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used
    /// to respond to S3 notifications.
    /// </summary>
    /// <param name="evnt">The event for the Lambda function handler to process.</param>
    /// <param name="context">The ILambdaContext that provides methods for logging and describing the Lambda environment.</param>
    /// <returns></returns>
    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        context.Logger.LogInformation($"A S3 event has been received, it contains {evnt.Records.Count} records");

        foreach (var s3Event in evnt.Records)
        {
            context.Logger.LogInformation($"Action: {s3Event.EventName}, Bucket {s3Event.S3.Bucket.Name}," +
                $"Key: {s3Event.S3.Object.Key}");

            if (!s3Event.EventName.Contains("Delete"))
            {
                try
                {
                    var response = await this.S3Client.GetObjectMetadataAsync(s3Event.S3.Bucket.Name,
                        s3Event.S3.Object.Key);

                    context.Logger.LogInformation($"The file type is {response.Headers.ContentType}");
                }
                catch (Exception ex)
                {
                    context.Logger.LogError(ex.Message);
                    context.Logger.LogError($"An exception ocorred while retrieving {s3Event.S3.Bucket.Name}/" +
                        $"{s3Event.S3.Object.Key}. Exception: {ex.Message}");
                }
            }
            else
            {
                context.Logger.LogInformation($"You deleted {s3Event.S3.Bucket.Name}/{s3Event.S3.Object.Key}");
            }
        }
    }
}