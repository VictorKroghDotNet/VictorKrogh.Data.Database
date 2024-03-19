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

    public async ValueTask<TModel?> GetAsync<TModel, TKey>(TKey key, int? commandTimeout = null)
        where TModel : Model
        where TKey : notnull
    {
        return await Connection.GetAsync<TModel>([key], Transaction, commandTimeout);
    }

    public async ValueTask<IEnumerable<TModel?>> GetAllAsync<TModel>(int? commandTimeout = null) where TModel : Model
    {
        return await Connection.GetAllAsync<TModel>(Transaction, commandTimeout);
    }

    public async ValueTask<bool> InsertAsync<TModel>(TModel model, int? commandTimeout = null) where TModel : Model
    {
        return await Connection.InsertAsync(model, Transaction, commandTimeout);
    }

    public async ValueTask<bool> UpdateAsync<TModel>(TModel model, int? commandTimeout = null) where TModel : Model
    {
        return await Connection.UpdateAsync(model, Transaction, commandTimeout);
    }

    public async ValueTask<bool> DeleteAsync<TModel>(TModel model, int? commandTimeout = null) where TModel : Model
    {
        return await Connection.DeleteAsync(model, Transaction, commandTimeout);
    }

    public async ValueTask<bool> DeleteAllAsync<TModel>(int? commandTimeout = null) where TModel : Model
    {
        return await Connection.DeleteAllAsync<TModel>(Transaction, commandTimeout);
    }

    public async ValueTask<IEnumerable<TModel?>> QueryAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return await Connection.QueryAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public async ValueTask<TModel> QueryFirstAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return await Connection.QueryFirstAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public async ValueTask<TModel?> QueryFirstOrDefaultAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return await Connection.QueryFirstOrDefaultAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public async ValueTask<TModel> QuerySingleAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return await Connection.QuerySingleAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public async ValueTask<TModel?> QuerySingleOrDefaultAsync<TModel>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null) where TModel : Model
    {
        return await Connection.QuerySingleOrDefaultAsync<TModel>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public async ValueTask<int> ExecuteAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Connection.ExecuteAsync(sql, parameters, Transaction, commandTimeout, commandType);
    }

    public async ValueTask<T?> ExecuteScalarAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        return await Connection.ExecuteScalarAsync<T>(sql, parameters, Transaction, commandTimeout, commandType);
    }

    [Obsolete("Use QuerySingleOrDefaultAsync<T>() instead!", true)]
    public ValueTask<T?> ExecuteSingleOrDefaultAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
    {
        throw new NotImplementedException();
    }

    [Obsolete("Use QueryAsync<T>() instead!", true)]
    public ValueTask<IEnumerable<T?>> ExecuteQueryAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
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
