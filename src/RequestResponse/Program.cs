using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Management;

namespace RequestResponse
{
    class Program
    {
        static async Task Main()
        {
            var connectionString = "<your_connection_string>";
            var requestQueueName = "sample.request";
            var replyQueueName = "sample.reply";

            // create queues
            await Task.WhenAll(CreateQueueAsync(connectionString, requestQueueName, false), CreateQueueAsync(connectionString, replyQueueName, true));

            // init threads
            var threads = new List<Thread>();
            for (var i = 0; i < 3; i++)
            {
                var threadId = "requestor-" + i;
                threads.Add( SampleThreadFactory.CreateRequestor(threadId, connectionString, requestQueueName, replyQueueName));
            }

            for (var i = 0; i < 2; i++)
            {
                var threadId = "replier-" + i;
                threads.Add(SampleThreadFactory.CreateReplier(threadId, connectionString, requestQueueName));
            }

            // start all
            Parallel.ForEach(threads, (thread, state) => thread.Start());

            Console.Read();
        }
        
        static async Task CreateQueueAsync(string connectionString, string queueName, bool requiresSession)
        {
            var managementClient = new ManagementClient(connectionString);

            if (await managementClient.QueueExistsAsync(queueName))
            {
                await managementClient.DeleteQueueAsync(queueName);
            }

            await managementClient.CreateQueueAsync(new QueueDescription(queueName)
            {
                RequiresSession = requiresSession
            });
        }
    }
}
