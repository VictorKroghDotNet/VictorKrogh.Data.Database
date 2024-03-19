using System.Data;
using VictorKrogh.Data.Database.Models;
using VictorKrogh.Data.Database.Providers;
using VictorKrogh.Data.Repositories;

namespace VictorKrogh.Data.Database.Repositories;

public abstract class DbReadOnlyRepositoryBase<TModel> : DbRepositoryBase, IReadOnlyRepository<TModel>
    where TModel : Model
{
    protected DbReadOnlyRepositoryBase(IDbProvider provider)
        : base(provider)
    {
    }

    protected async virtual ValueTask<TModel?> QuerySingleAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.QuerySingleAsync<TModel>(sql, parameters, commandTimeout, commandType);
    }

    protected async virtual ValueTask<TModel?> QuerySingleOrDefaultAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.QuerySingleOrDefaultAsync<TModel>(sql, parameters, commandTimeout, commandType);
    }

    protected async virtual ValueTask<TModel?> QueryFirstAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.QueryFirstAsync<TModel>(sql, parameters, commandTimeout, commandType);
    }

    protected async virtual ValueTask<TModel?> QueryFirstOrDefaultAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.QueryFirstOrDefaultAsync<TModel>(sql, parameters, commandTimeout, commandType);
    }

    protected async virtual ValueTask<IEnumerable<TModel?>> QueryAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.QueryAsync<TModel>(sql, parameters, commandTimeout, commandType);
    }

    public abstract ValueTask<IEnumerable<TModel?>> GetAsync();

    public async virtual ValueTask<TModel?> GetFirstOrDefaultAsync()
    {
        var result = await GetAsync();
        if (result == default)
        {
            return default;
        }

        return result.FirstOrDefault();
    }
}

public abstract class DbReadOnlyRepositoryBase<TModel, TKey> : DbReadOnlyRepositoryBase<TModel>, IReadOnlyRepository<TModel, TKey>
    where TModel : Model
    where TKey : notnull
{
    protected DbReadOnlyRepositoryBase(IDbProvider provider)
        : base(provider)
    {
    }

    public async ValueTask<TModel?> GetAsync(TKey key)
    {
        return await Provider.GetAsync<TModel, TKey>(key);
    }

    public override async ValueTask<IEnumerable<TModel?>> GetAsync()
    {
        return await Provider.GetAllAsync<TModel>();
    }
}
