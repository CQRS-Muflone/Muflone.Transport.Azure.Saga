using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Muflone.Messages.Commands;
using Muflone.Transport.Azure.Factories;
using Muflone.Transport.Azure.Models;
using System.Globalization;
using Muflone.Saga;
using Muflone.Transport.Azure.Saga.Abstracts;

namespace Muflone.Transport.Azure.Saga.Consumers;

public abstract class SagaStartedByConsumerBase<TCommand> : ISagaStartedByConsumer<TCommand>, IAsyncDisposable
	where TCommand : Command
{
	public string TopicName { get; }

	private readonly ServiceBusProcessor _processor;
	private readonly Persistence.ISerializer _messageSerializer;
	private readonly ILogger _logger;

	protected abstract ISagaStartedByAsync<TCommand> HandlerAsync { get; }

	protected SagaStartedByConsumerBase(AzureServiceBusConfiguration azureServiceBusConfiguration,
		ILoggerFactory loggerFactory,
		Persistence.ISerializer? messageSerializer = null)
	{
		TopicName = typeof(TCommand).Name;

		_logger = loggerFactory.CreateLogger(GetType()) ?? throw new ArgumentNullException(nameof(loggerFactory));

		var serviceBusClient = new ServiceBusClient(azureServiceBusConfiguration.ConnectionString);
		_messageSerializer = messageSerializer ?? new Persistence.Serializer();

		// Create Queue on Azure ServiceBus if missing
		ServiceBusAdministrator.CreateQueueIfNotExistAsync(new AzureQueueReferences(TopicName, "",
			azureServiceBusConfiguration.ConnectionString)).GetAwaiter().GetResult();

		_processor = serviceBusClient.CreateProcessor(
			topicName: TopicName.ToLower(CultureInfo.InvariantCulture),
			subscriptionName: "", new ServiceBusProcessorOptions
			{
				AutoCompleteMessages = false,
				MaxConcurrentCalls = azureServiceBusConfiguration.MaxConcurrentCalls
			});
		_processor.ProcessMessageAsync += AzureMessageHandler;
		_processor.ProcessErrorAsync += ProcessErrorAsync;
	}

	public Task ConsumeAsync(TCommand message, CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));

		return ConsumeAsyncCore(message, cancellationToken);
	}

	private Task ConsumeAsyncCore<T>(T message, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();
		
		try
		{
			ArgumentNullException.ThrowIfNull(message);

			HandlerAsync.StartedByAsync((dynamic)message);

			return Task.CompletedTask;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex,
				"An error occurred processing command {Name}. StackTrace: {ExStackTrace} - Source: {ExSource} - Message: {ExMessage}",
				typeof(T).Name, ex.StackTrace, ex.Source, ex.Message);
			throw;
		}
	}

	public async Task StartAsync(CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		await _processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
	}

	public Task StopAsync(CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		return _processor.StopProcessingAsync(cancellationToken);
	}

	private async Task AzureMessageHandler(ProcessMessageEventArgs args)
	{
		try
		{
			_logger.LogInformation("Received message \'{MessageMessageId}\'. Processing...", args.Message.MessageId);

			var message = await _messageSerializer.DeserializeAsync<TCommand>(args.Message.Body.ToString());

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