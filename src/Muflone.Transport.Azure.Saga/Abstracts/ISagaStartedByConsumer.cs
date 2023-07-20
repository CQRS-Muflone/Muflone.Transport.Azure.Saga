using Muflone.Messages.Commands;
using Muflone.Transport.Azure.Abstracts;

namespace Muflone.Transport.Azure.Saga.Abstracts;

public interface ISagaStartedByConsumer<in T> : IConsumer where T : Command
{
	Task ConsumeAsync(T message, CancellationToken cancellationToken = default);
}