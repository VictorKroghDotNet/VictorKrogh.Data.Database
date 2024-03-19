using System.Data;
using VictorKrogh.Data.Database.Models;
using VictorKrogh.Data.Database.Providers;
using VictorKrogh.Data.Repositories;

namespace VictorKrogh.Data.Database.Repositories;

public abstract class DbRepositoryBase : RepositoryBase<IDbProvider>
{
    protected DbRepositoryBase(IDbProvider provider) 
        : base(provider)
    {
    }

    protected async ValueTask<int> ExecuteAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.ExecuteAsync(sql, parameters, commandTimeout, commandType);
    }

    protected async ValueTask<T?> ExecuteScalarAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.ExecuteScalarAsync<T>(sql, parameters, commandTimeout, commandType);
    }

    protected async ValueTask<T?> ExecuteSingleOrDefaultAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.ExecuteSingleOrDefaultAsync<T>(sql, parameters, commandTimeout, commandType);
    }

    protected async ValueTask<IEnumerable<T?>> ExecuteQueryAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Provider.ExecuteQueryAsync<T>(sql, parameters, commandTimeout, commandType);
    }
}

public abstract class DbRepositoryBase<TModel, TKey> : DbReadOnlyRepositoryBase<TModel, TKey>, IRepository<TModel, TKey>
    where TModel : Model
    where TKey : notnull
{
    protected DbRepositoryBase(IDbProvider provider)
        : base(provider)
    {
    }

    public async virtual ValueTask<bool> AddAsync(TModel model)
    {
        return await Provider.InsertAsync(model);
    }

    public async virtual ValueTask<bool> UpdateAsync(TModel model)
    {
        return await Provider.UpdateAsync(model);
    }

    public async virtual ValueTask<bool> AddOrUpdateAsync(TModel model)
    {
        if (model.IsTransient())
        {
            return await AddAsync(model);
        }

        return await UpdateAsync(model);
    }

    public async virtual ValueTask<bool> DeleteAsync(TModel model)
    {
        return await Provider.DeleteAsync(model);
    }

    public async virtual ValueTask<bool> DeleteAllAsync()
    {
        return await Provider.DeleteAllAsync<TModel>();
    }
}