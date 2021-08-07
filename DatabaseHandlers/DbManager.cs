

namespace rokono_cl.DatabaseHandlers
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;
    using MSSQLTOMYSQLConverter.DatabaseHandlers.Databases;
    using MSSQLTOMYSQLConverter.Models;
    using RokonoDbManager.Models;

    public class DbManager : IDisposable
    {
        SqlConnection SqlConnection;
        public List<BindingRowModel> _localData { get; set; }
        public DbManager(string connectionString)
        {
            _localData = new List<BindingRowModel>();
            SqlConnection = new SqlConnection(connectionString);
        }

        internal async Task<List<string>> GetTables()
        {
            var result = new List<string>();
            var query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'";
            using(var reader = await ExecuteQuery(query))
            {
                    while (await reader.ReadAsync()) 
                    {   
                        result.Add(reader.GetString(0));
                    }
            }
            SqlConnection.Close();
            return result;
        }

        public string GetDbUmlData(int databaseType)
        {
            var result = string.Empty;
            var query = "SELECT tp.name 'Parent table', cp.name 'Column Id',tr.name 'Refrenced table',cr.name 'Corelation Name' FROM  sys.foreign_keys fk INNER JOIN  sys.tables tp ON fk.parent_object_id = tp.object_id INNER JOIN  sys.tables tr ON fk.referenced_object_id = tr.object_id INNER JOIN  sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id INNER JOIN sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id INNER JOIN sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id ORDER BY tp.name, cp.column_id";
            SqlCommand command = new SqlCommand(query, SqlConnection);
            
            // Open the connection in a try/catch block. 
            // Create and execute the DataReader, writing the result
            // set to the console window.
            try
            {
                SqlConnection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                 
                    if(databaseType != 2)
                        result += $"ALTER TABLE {reader.GetString(0)} ADD FOREIGN KEY ({reader.GetString(1)}) REFERENCES {reader.GetString(2)}({reader.GetString(3)});\r\n";
                    else
                    {

                    }
                }
                reader.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return result;
        }

        internal string GetTableRows(string x)
        {
            var tableQuery = string.Empty;
            var query = $"SELECT * FROM {x}";
            SqlCommand command = new SqlCommand(query, SqlConnection);
            
            // Open the connection in a try/catch block. 
            // Create and execute the DataReader, writing the result
            // set to the console window.
            try
            {

                SqlConnection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {   
                    tableQuery += $"Insert into {x} values (";
                    
                    for(var i= 0; i < reader.FieldCount -1; i++)
                    {
                        var val =  reader.GetValue(i);
                        if(i != reader.FieldCount - 1)
                            tableQuery += $"{GetValueByType(val)},";
                        else
                            tableQuery += $"{GetValueByType(val)}";
                    }
                    tableQuery += ");\r\n";
                }
                reader.Close();
                SqlConnection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return tableQuery;
        }

        private object GetValueByType(object val)
        {
            if(val is string)
                val = $"'{val}'";
            if(val is DateTime)
                val = $"'{val}'";
            if(val == null)
                val = "null";
            return val;
        }

        public List<OutboundTableConnection> GetTableForignKeys()
        {
            var result = new List<OutboundTableConnection> ();
            var query = "SELECT tp.name 'Parent table', cp.name 'Column Id',tr.name 'Refrenced table',cr.name 'Corelation Name' FROM  sys.foreign_keys fk INNER JOIN  sys.tables tp ON fk.parent_object_id = tp.object_id INNER JOIN  sys.tables tr ON fk.referenced_object_id = tr.object_id INNER JOIN  sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id INNER JOIN sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id INNER JOIN sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id ORDER BY tp.name, cp.column_id";
            SqlCommand command = new SqlCommand(query, SqlConnection);

            // Open the connection in a try/catch block. 
            // Create and execute the DataReader, writing the result
            // set to the console window.
            try
            {
                SqlConnection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    result.Add(new OutboundTableConnection{
                            TableName = reader.GetString(2),
                            ConnectionName = reader.GetString(3)

                    });
                        
                    //  result += $"ALTER TABLE {reader.GetString(0)} ADD FOREIGN KEY ({reader.GetString(1)}) REFERENCES {reader.GetString(2)}({reader.GetString(3)});";
                }
                reader.Close();
                SqlConnection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return result;
        }
        
        
        public async Task<OutboundTable> GetTableData(string tableName, List<OutboundTableConnection> foreginKeys, int databaseType)
        {
            var result = new OutboundTable();
         
            var primaryAutoInc = string.Empty;
            
            using(var primaryReader = await ExecuteQuery($"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tableName}'and COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1"))
            {
                while (await primaryReader.ReadAsync())    
                {
                    primaryAutoInc = primaryReader.GetString(0);
                }
            }
            SqlConnection.Close();

      
            var query =$"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tableName}'";
            using(var reader = await ExecuteQuery(query))
            {
            
                var tableData = $"CREATE TABLE IF NOT EXISTS {tableName} (";
                var i = 0;
                switch(databaseType)
                {
                    case 1:
                    _localData = await GetMysqlProvider(_localData,reader,foreginKeys,tableName,primaryAutoInc);
                    break;
                    case 2:
                    _localData = await GetSQliteProvider(_localData,reader,foreginKeys,tableName,primaryAutoInc);
                    break;
                }
                var lastRow = _localData.Count;
                _localData.Where(x=>!x.SpecialRow && !x.IsFK).ToList().ForEach(x=>{
                    i++;
                    var next = ",";
                    if(i == lastRow)
                        next = "";
                    if(x.TableName == primaryAutoInc)
                        tableData += $"{x.TableName} {x.DataType}{next}";
                    else
                        tableData += $"{x.TableName} {x.DataType} {x.IsNull}{next}";
                });
                if(databaseType == 2)
                {
                    _localData.Where(x => x.SpecialRow).ToList().ForEach(x =>
                    {
                        tableData += x.TableName;
                    });
                }
                tableData += " );";
                _localData = new List<BindingRowModel>();
                result.CreationgString = tableData;
            }
            SqlConnection.Close();

            return result;
        }

        private async Task<List<BindingRowModel>> GetMysqlProvider(
            List<BindingRowModel> _localData,
            SqlDataReader reader,
            List<OutboundTableConnection> foreginKeys,
            string tableName,
            string primaryAutoInc)
        {
            var result =  new List<BindingRowModel>();
            await using (var sqlProvider =  new MySQL(_localData,reader,foreginKeys,tableName,primaryAutoInc))
            {
               result = await sqlProvider.ReadDataResultAsync();
            }
            return result;
        }


        private async Task<List<BindingRowModel>> GetSQliteProvider(
            List<BindingRowModel> _localData,
            SqlDataReader reader,
            List<OutboundTableConnection> foreginKeys,
            string tableName,
            string primaryAutoInc)
        {
            var result =  new List<BindingRowModel>();
            await using (var sqlProvider =  new SQLITE(_localData,reader,foreginKeys,tableName,primaryAutoInc))
            {
               result = await sqlProvider.ReadDataResultAsync();
            }
            return result;
        }


        public async Task<SqlDataReader> ExecuteQuery(string query)
        {
            
            SqlCommand command = new SqlCommand(query, SqlConnection);
            try
            {
                SqlConnection.Open();
                var result =  await command.ExecuteReaderAsync();
                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }
        }
        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual async Task Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    await SqlConnection.DisposeAsync();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~DbManager()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public async void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
             await Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion

    }
}