using Netcorext.Mediator.Queuing;

namespace Netcorext.Mediator.Internals;

internal class DefaultQueuing : IQueuing
{
    public async Task<string> PublishAsync<TResult>(IRequest<TResult> request, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public async Task SubscribeAsync(string[] channels, Action<string, object> handler, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}