using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;

namespace RequestResponse
{
    class Program
    {
        const string ConnectionString = "<your_connection_string>";
        const string RequestQueueName = "sample.request";
        const string ReplyQueueName = "sample.reply";

        static async Task Main()
        {
            // create queues
            await Task.WhenAll(CreateQueueAsync(RequestQueueName, false), CreateQueueAsync(ReplyQueueName, true));

            // init threads
            var threads = new List<Thread>();
            for (var i = 0; i < 3; i++)
            {
                var threadId = "requestor-" + i;
                threads.Add(new Thread(async () => await GetRequestorThread(threadId)));
            }

            for (var i = 0; i < 2; i++)
            {
                var threadId = "replier-" + i;
                threads.Add(new Thread(() => GetReplierThread(threadId)));
            }

            // start all
            Parallel.ForEach(threads, (thread, state) => thread.Start());

            Console.Read();
        }

        private static async Task GetRequestorThread(string threadId)
        {
            var messageSender = new MessageSender(ConnectionString, RequestQueueName);
            var sessionId = Guid.NewGuid().ToString();

            var message = new Message
            {
                MessageId = threadId,
                ReplyTo = new ServiceBusConnectionStringBuilder(ConnectionString) { EntityPath = ReplyQueueName }.ToString(),
                ReplyToSessionId = sessionId,
                TimeToLive = TimeSpan.FromMinutes(2)
            };

            LogClient($"{threadId} send a message to '{RequestQueueName}' with replyToSessionId='{message.ReplyToSessionId}' and entityPath='{ReplyQueueName}'", "send", message);
            await messageSender.SendAsync(message);

            SessionClient sessionClient = new SessionClient(ConnectionString, ReplyQueueName);
            var session = await sessionClient.AcceptMessageSessionAsync(sessionId);

            LogClient($"{threadId}'s waiting a reply message from '{ReplyQueueName}' with sessionId='{sessionId}'...", "wait", null);
            Message sessionMessage = await session.ReceiveAsync();

            LogClient($"{threadId} received a reply message from '{ReplyQueueName}' with sessionId='{sessionMessage.SessionId}'", "receive", sessionMessage);
        }

        private static void GetReplierThread(string threadId)
        {
            var messageReceiver = new MessageReceiver(ConnectionString, RequestQueueName);

            messageReceiver.RegisterMessageHandler(
                async (message, cancellationToken) =>
                {
                    LogWorker($"{threadId} received a reply message from '{RequestQueueName}' with replyToSessionId='{message.ReplyToSessionId}'", "receive", message);
                    var connectionStringBuilder = new ServiceBusConnectionStringBuilder(message.ReplyTo);
                    var replyToQueue = new MessageSender(connectionStringBuilder);
                    var replyMessage = new Message(Encoding.UTF8.GetBytes($"processed by {threadId}"))
                    {
                        CorrelationId = message.MessageId,
                        SessionId = message.ReplyToSessionId,
                        TimeToLive = TimeSpan.FromMinutes(2)
                    };

                    /****  Simulate an action   *****/
                    await Task.Delay(new Random().Next(1000, 2000), cancellationToken);
                    /*******************************/

                    LogWorker($"{threadId} send a reply message to '{connectionStringBuilder.EntityPath}' with sessionId='{message.ReplyToSessionId}'", "send", replyMessage);
                    await replyToQueue.SendAsync(replyMessage);
                },
                new MessageHandlerOptions(args => throw args.Exception)
                {
                    MaxConcurrentCalls = 1
                });
        }


        #region helper 

        private static void LogClient(string logMessage, string action, Message message)
        {
            Log(logMessage, action, message, ConsoleColor.Cyan);
        }

        private static void LogWorker(string logMessage, string action, Message message)
        {
            Log(logMessage, action, message, ConsoleColor.Red);
        }

        private static void Log(string logMessage, string action, Message message, ConsoleColor color)
        {
            lock (Console.Out)
            {
                Console.ForegroundColor = color;
                Console.WriteLine($"- {logMessage} [{DateTime.Now:hh:mm:ss.fff tt}]");
                Console.ResetColor();
                if (action != null)
                    Console.WriteLine($"\t action={action}");
                if (message?.Body != null)
                    Console.WriteLine($"\t Body={ Encoding.UTF8.GetString(message.Body) }");
                if (message?.MessageId != null)
                    Console.WriteLine($"\t MessageId={ message.MessageId}");
                if (message?.CorrelationId != null)
                    Console.WriteLine($"\t CorrelationId={ message.CorrelationId}");
                if (message?.SessionId != null)
                    Console.WriteLine($"\t SessionId={ message.SessionId}");
                if (message?.ReplyToSessionId != null)
                    Console.WriteLine($"\t ReplyToSessionId={ message.ReplyToSessionId}");
                Console.WriteLine();
            }
        }

        static async Task CreateQueueAsync(string queueName, bool requiresSession)
        {
            var managementClient = new ManagementClient(ConnectionString);

            if (await managementClient.QueueExistsAsync(queueName))
            {
                await managementClient.DeleteQueueAsync(queueName);
            }

            await managementClient.CreateQueueAsync(new QueueDescription(queueName)
            {
                RequiresSession = requiresSession
            });
        }
        #endregion
    }
}
