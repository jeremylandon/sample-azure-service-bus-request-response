using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;

namespace RequestResponse
{
    public static class SampleThreadFactory
    {
        public static Thread CreateRequestor(string threadId, string connectionString, string requestQueueName, string replyQueueName)
        {
            return new Thread(async () =>
            {
                /*** send message ***/
                var messageSender = new MessageSender(connectionString, requestQueueName);
                var sessionId = "session-" + threadId;

                var message = new Message
                {
                    MessageId = threadId,
                    ReplyTo = new ServiceBusConnectionStringBuilder(connectionString) { EntityPath = replyQueueName }.ToString(),
                    ReplyToSessionId = sessionId,
                    TimeToLive = TimeSpan.FromMinutes(2)
                };

                LogClient($"{threadId} send a message to '{requestQueueName}' with replyToSessionId='{message.ReplyToSessionId}' and entityPath='{replyQueueName}'", "send", message);
                
                await messageSender.SendAsync(message);
                await messageSender.CloseAsync();
                /*** send message ***/

                /*** wait response ***/
                SessionClient sessionClient = new SessionClient(connectionString, replyQueueName);
                var session = await sessionClient.AcceptMessageSessionAsync(sessionId);

                LogClient($"{threadId}'s waiting a reply message from '{replyQueueName}' with sessionId='{sessionId}'...", "wait", null);
                
                Message sessionMessage = await session.ReceiveAsync(TimeSpan.FromMinutes(2));

                LogClient($"{threadId} received a reply message from '{replyQueueName}' with sessionId='{sessionMessage.SessionId}'", "receive", sessionMessage);

                await session.CompleteAsync(sessionMessage.SystemProperties.LockToken);
                await session.CloseAsync();
                await sessionClient.CloseAsync();
                /*** wait response ***/
            });
        }

        public static Thread CreateReplier(string threadId, string connectionString, string requestQueueName)
        {
            return new Thread(() =>
            {
                var messageReceiver = new MessageReceiver(connectionString, requestQueueName);

                messageReceiver.RegisterMessageHandler(
                     async (message, cancellationToken) =>
                     {
                         LogWorker($"{threadId} received a reply message from '{requestQueueName}' with replyToSessionId='{message.ReplyToSessionId}'", "receive", message);
                         
                         var connectionStringBuilder = new ServiceBusConnectionStringBuilder(message.ReplyTo);
                         var replyToQueue = new MessageSender(connectionStringBuilder);
                         var replyMessage = new Message(Encoding.UTF8.GetBytes($"processed by {threadId}"))
                         {
                             CorrelationId = message.MessageId,
                             SessionId = message.ReplyToSessionId,
                             TimeToLive = TimeSpan.FromMinutes(2)
                         };

                         /****  Simulate an action  *****/
                         await Task.Delay(new Random().Next(1000, 2000), cancellationToken);
                         /*******************************/

                         LogWorker($"{threadId} send a reply message to '{connectionStringBuilder.EntityPath}' with sessionId='{message.ReplyToSessionId}'", "send", replyMessage);
                         
                         await replyToQueue.SendAsync(replyMessage);
                     },
                     new MessageHandlerOptions(args => throw args.Exception)
                     {
                         MaxConcurrentCalls = 1
                     });
            });
        }

        #region log
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
        #endregion
    }
}
