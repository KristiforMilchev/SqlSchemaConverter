
namespace MSSQLTOMYSQLConverter.DatabaseHandlers.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;
    using MSSQLTOMYSQLConverter.Models;
    using RokonoDbManager.Models;

    public class SQLITE : IAsyncDisposable
    {
        private List<BindingRowModel> _localData { get; set; }
        private SqlDataReader _reader {get; set;}
        List<OutboundTableConnection> _foreginKeys {get; set;}
        string _tableName {get; set;}
        string _primaryAutoInc {get; set;}
        
        public SQLITE(List<BindingRowModel> data, SqlDataReader reader,  List<OutboundTableConnection> keys, string tableName, string primaryAutoInc)
        {
            _localData = data;
            _reader = reader;
            _foreginKeys = keys;
            _tableName = tableName;
            _primaryAutoInc = primaryAutoInc;
        }

        public async Task< List<BindingRowModel>> ReadDataResultAsync()
        {
            var result = new List<BindingRowModel>();
            var notNull = "NOT NULL";
            while (await _reader.ReadAsync())    
            {
                
                if(_reader.GetString(3) == "NO")
                    notNull = "NOT NULL";
                else
                    notNull = "";
                
                if(_reader.GetString(0) == _primaryAutoInc)
                    _localData.Add(new BindingRowModel{
                        TableName = _reader.GetString(0),
                        DataType = $"INTEGER AUTOINCREMENT",
                        IsNull = notNull
                    });
                else if(_foreginKeys.Any(x=>x.TableName == _tableName && x.ConnectionName == _reader.GetString(0)))
                    _localData.Add(new BindingRowModel{
                        TableName = _reader.GetString(0),
                        DataType = $"INTEGER AUTOINCREMENT",
                        IsNull = notNull
                    });
                else if(_reader.IsDBNull(2))
                    _localData.Add(new BindingRowModel{
                        TableName = _reader.GetString(0),
                        DataType = $"{DetermineType(_reader.GetString(1), _reader.IsDBNull(2) ? -1 : _reader.GetInt32(2))}",
                        IsNull = notNull
                    });
            }
            return result;
        }
        private  string DetermineType(string value, int valueLenght)
        {
            var res = string.Empty;
            var lenght = valueLenght != -1 ? $"({valueLenght.ToString()})" : "";

            switch(value)
            {
                case "char":
                   res = "TEXT";
                    break;
                case "varchar":
                   res = "TEXT";
                    break;
                case "text":
                    res = $"TEXT";
                    break;
                case "nchar":
                    res = "TEXT";
                    break;
                case "nvarchar":
                     res = "TEXT";
                    break;  
                case "ntext":
                    res = "TEXT";
                    break;
                case "binary":
                    res = "BLOB";
                    break;
                case "varbinary":
                    res = "BLOB";
                    break;
                case "varbinary(max)":
                    res = "BLOB";
                    break;
                case "image":
                    res = "BLOB";
                    break;
                case "bit":
                    res = "BLOB"; 
                    break;
                case "tinyint":
                    res = "INTEGER";
                    break;
                case "smallint":
                    res = "INTEGER";
                    break;
                case "int":
                    res = "INTEGER";
                    break;
                case "bigint":
                    res = "INTEGER";
                    break;
                case "decimal":
                    res = "REAL";
                    break;
                case "numeric":
                    res = "REAL";
                    break;
                case "smallmoney":
                    res = "REAL";
                    break;
                case "money":
                    res = "REAL";
                    break;
                case "float":
                    res = "REAL";
                    break;
                case "real":
                    res = "REAL";
                    break;
                case "datetime":
                    res = $"DATETIME";
                    break;
                case "datetime2":
                    res = $"DATETIME";
                    break;
                case "smalldatetime":
                    res = $"DATETIME";
                    break;
                case "date":
                    res = $"DATE";
                    break;
                case "time":
                    res = "DATETIME";
                    break;
                case "datetimeoffset":
                    res = "DATETIME";
                    break;
                case "timestamp":
                    res = "DATETIME";
                    break;
            }
            return res;
        }
        public async ValueTask DisposeAsync()
        {
            await _reader.DisposeAsync();
            _foreginKeys.Clear();
            _primaryAutoInc = null;
            _tableName = null;
            GC.SuppressFinalize(this);
        }
    }
}