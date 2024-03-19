using Dapper;
using Dapper.Contrib.Extensions;
using System.Data;
using VictorKrogh.Data.Database.Models;
using VictorKrogh.Data.Providers;

namespace VictorKrogh.Data.Database.Providers;

public abstract class DbProviderBase(IsolationLevel isolationLevel) : ProviderBase(isolationLevel), IDbProvider
{
    private IDbConnection? connection;
    private IDbTransaction? transaction;
    private bool committed;

    public IDbConnection Connection => connection ??= CreateConnection();

    public IDbTransaction Transaction
    {
        get
        {
            if (transaction is not null)
            {
                return transaction;
            }

            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }

            return transaction = Connection.BeginTransaction();
        }
    }

    protected abstract IDbConnection CreateConnection();

    public string GetQualifiedTableName<TModel>() where TModel : Model
    {
        return Connection.GetQualifiedTableName<TModel>();
    }

    public Task<TModel?> GetAsync<TModel, TKey>(TKey key, int? commandTimeout = null)
        where TModel : Model
        where TKey : notnull
    {
        return Connection.GetAsync<TModel>([key], Transaction, commandTimeout);
    }

    public async Task<IEnumerable<TModel?>> GetAllAsync<TModel>(int? commandTimeout = null) where TModel : Model
    {
        return await Connection.GetAllAsync<TModel>(Transaction, commandTimeout);
    }

    public Task<bool> InsertAsync<TModel>(TModel model, int? commandTimeout = null) where TModel : Model
    {
        return Connection.InsertAsync(model, Transaction, commandTimeout);
    }

    public Task<bool> UpdateAsync<TModel>(TModel model, int? commandTimeout = null) where TModel : Model
    {
        return Connection.UpdateAsync(model, Transaction, commandTimeout);
    }

    public Task<bool> DeleteAsync<TModel>(TModel model, int? commandTimeout = null) where TModel : Model
    {
        return Connection.DeleteAsync(model, Transaction, commandTimeout);
    }

    public Task<bool> DeleteAllAsync<TModel>(int? commandTimeout = null) where TModel : Model
    {
        return Connection.DeleteAllAsync<TModel>(Transaction, commandTimeout);
    }

    public async Task<IEnumerable<TModel?>> QueryAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return await Connection.QueryAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public Task<TModel> QueryFirstAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return Connection.QueryFirstAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public Task<TModel?> QueryFirstOrDefaultAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return Connection.QueryFirstOrDefaultAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public Task<TModel> QuerySingleAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return Connection.QuerySingleAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public Task<TModel?> QuerySingleOrDefaultAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return Connection.QuerySingleOrDefaultAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public Task<int> ExecuteAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return Connection.ExecuteAsync(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public Task<T?> ExecuteScalarAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return Connection.ExecuteScalarAsync<T>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    [Obsolete("Use QuerySingleOrDefaultAsync<T>() instead!", true)]
    public Task<T?> ExecuteSingleOrDefaultAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        throw new NotImplementedException();
    }

    [Obsolete("Use QueryAsync<T>() instead!", true)]
    public Task<IEnumerable<T?>> ExecuteQueryAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        throw new NotImplementedException();
    }

    public override void Commit()
    {
        if (transaction == null)
        {
            return;
        }

        transaction.Commit();
        committed = true;
    }

    public override void Rollback()
    {
        if (transaction == null)
        {
            return;
        }

        transaction.Rollback();
    }

    protected override void DisposeManagedState()
    {
        if (transaction != null)
        {
            if (!committed)
            {
                Rollback();
            }

            transaction.Dispose();
        }

        if (connection != null)
        {
            if (connection.State != ConnectionState.Closed)
            {
                connection.Close();
            }

            connection.Dispose();
        }
    }
}
