﻿using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Muflone.Messages.Events;
using Muflone.Transport.Azure.Abstracts;
using Muflone.Transport.Azure.Factories;
using Muflone.Transport.Azure.Models;
using System.Globalization;

namespace Muflone.Transport.Azure.Saga.Consumers;

public abstract class SagaEventConsumerBase<T> : IDomainEventConsumer<T>, IAsyncDisposable where T : class, IDomainEvent
{
	public string TopicName { get; }

	private readonly ServiceBusProcessor _processor;
	private readonly Persistence.ISerializer _messageSerializer;
	private readonly ILogger _logger;

	protected abstract IEnumerable<IDomainEventHandlerAsync<T>> HandlersAsync { get; }

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
		ServiceBusAdministrator.CreateTopicIfNotExistAsync(azureServiceBusConfiguration).GetAwaiter().GetResult();

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

	public async Task StartAsync(CancellationToken cancellationToken = default) =>
		await _processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);

	public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

	public async Task ConsumeAsync(T message, CancellationToken cancellationToken = default)
	{
		try
		{
			if (message == null)
				throw new ArgumentNullException(nameof(message));

			foreach (var handlerAsync in HandlersAsync)
			{
				await handlerAsync.HandleAsync((dynamic)message, cancellationToken);
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex,
				$"An error occurred processing domainEvent {TopicName}. StackTrace: {ex.StackTrace} - Source: {ex.Source} - Message: {ex.Message}");
			throw;
		}
	}

	private async Task AzureMessageHandler(ProcessMessageEventArgs args)
	{
		try
		{
			_logger.LogInformation($"Received message '{args.Message.MessageId}'. Processing...");

			var message = await _messageSerializer.DeserializeAsync<T>(args.Message.Body.ToString());

			await ConsumeAsync(message, args.CancellationToken);

			await args.CompleteMessageAsync(args.Message).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, $"an error has occurred while processing message '{args.Message.MessageId}': {ex.Message}");
			if (args.Message.DeliveryCount > 3)
				await args.DeadLetterMessageAsync(args.Message).ConfigureAwait(false);
			else
				await args.AbandonMessageAsync(args.Message).ConfigureAwait(false);
		}
	}

	private Task ProcessErrorAsync(ProcessErrorEventArgs arg)
	{
		_logger.LogError(arg.Exception,
			$"An exception has occurred while processing message '{arg.FullyQualifiedNamespace}'");
		return Task.CompletedTask;
	}

	#region Dispose

	public ValueTask DisposeAsync() => ValueTask.CompletedTask;

	#endregion
}