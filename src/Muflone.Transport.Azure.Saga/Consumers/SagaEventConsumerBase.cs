using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Muflone.Messages.Events;
using Muflone.Transport.Azure.Factories;
using Muflone.Transport.Azure.Models;
using System.Globalization;
using Muflone.Saga;
using Muflone.Transport.Azure.Saga.Abstracts;

namespace Muflone.Transport.Azure.Saga.Consumers;

public abstract class SagaEventConsumerBase<T> : ISagaEventConsumer<T>, IAsyncDisposable
	where T : Event
{
	private string TopicName { get; }

	private readonly ServiceBusProcessor _processor;
	private readonly Persistence.ISerializer _messageSerializer;
	private readonly ILogger _logger;

	protected abstract ISagaEventHandlerAsync<T> HandlerAsync { get; }

	protected SagaEventConsumerBase(AzureServiceBusConfiguration azureServiceBusConfiguration,
		ILoggerFactory loggerFactory,
		Persistence.ISerializer? messageSerializer = null)
	{
		TopicName = typeof(T).Name;

		_logger = loggerFactory.CreateLogger(GetType()) ?? throw new ArgumentNullException(nameof(loggerFactory));
		_messageSerializer = messageSerializer ?? new Persistence.Serializer();

		if (string.IsNullOrWhiteSpace(azureServiceBusConfiguration.ClientId))
			throw new ArgumentNullException(nameof(azureServiceBusConfiguration.ClientId));

		// Create Topic on Azure ServiceBus if missing
		azureServiceBusConfiguration = new AzureServiceBusConfiguration(azureServiceBusConfiguration.ConnectionString,
			TopicName.ToLower(CultureInfo.InvariantCulture), azureServiceBusConfiguration.ClientId);
		ServiceBusAdministrator
			.CreateTopicIfNotExistAsync(azureServiceBusConfiguration).GetAwaiter().GetResult();

		var serviceBusClient = new ServiceBusClient(azureServiceBusConfiguration.ConnectionString);
		_processor = serviceBusClient.CreateProcessor(
			topicName: TopicName.ToLower(CultureInfo.InvariantCulture),
			subscriptionName: $"{azureServiceBusConfiguration.ClientId}-subscription", new ServiceBusProcessorOptions
			{
				AutoCompleteMessages = false,
				MaxConcurrentCalls = azureServiceBusConfiguration.MaxConcurrentCalls
			});
		_processor.ProcessMessageAsync += AzureMessageHandler;
		_processor.ProcessErrorAsync += ProcessErrorAsync;
	}

	public async Task StartAsync(CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		await _processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
	}

	public Task StopAsync(CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		return Task.CompletedTask;
	}

	public async Task ConsumeAsync(T message, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		
		try
		{
			if (message == null)
				throw new ArgumentNullException(nameof(message));

			await HandlerAsync.HandleAsync((dynamic)message);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex,
				"An error occurred processing domainEvent {TopicName}. StackTrace: {ExStackTrace} - Source: {ExSource} - Message: {ExMessage}",
				TopicName, ex.StackTrace, ex.Source, ex.Message);
			throw;
		}
	}

	private async Task AzureMessageHandler(ProcessMessageEventArgs args)
	{
		try
		{
			_logger.LogInformation("Received message \'{MessageMessageId}\'. Processing...", args.Message.MessageId);

			var message = await _messageSerializer.DeserializeAsync<T>(args.Message.Body.ToString());

			await ConsumeAsync(message!, args.CancellationToken);

			await args.CompleteMessageAsync(args.Message).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "an error has occurred while processing message \'{MessageMessageId}\': {ExMessage}",
				args.Message.MessageId, ex.Message);
			if (args.Message.DeliveryCount > 3)
				await args.DeadLetterMessageAsync(args.Message).ConfigureAwait(false);
			else
				await args.AbandonMessageAsync(args.Message).ConfigureAwait(false);
		}
	}

	private Task ProcessErrorAsync(ProcessErrorEventArgs arg)
	{
		_logger.LogError(arg.Exception,
			"An exception has occurred while processing message \'{ArgFullyQualifiedNamespace}\'",
			arg.FullyQualifiedNamespace);
		return Task.CompletedTask;
	}

	#region Dispose

	public ValueTask DisposeAsync() => ValueTask.CompletedTask;

	#endregion
}