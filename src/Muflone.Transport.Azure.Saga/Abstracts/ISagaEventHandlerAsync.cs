using Muflone.Messages.Events;
using Muflone.Transport.Azure.Abstracts;

namespace Muflone.Transport.Azure.Saga.Abstracts;

public interface ISagaEventConsumer<in T> : IConsumer where T : Event
{
	Task ConsumeAsync(T message, CancellationToken cancellationToken = default);
}