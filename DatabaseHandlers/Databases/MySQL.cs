namespace MSSQLTOMYSQLConverter.DatabaseHandlers.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;
    using MSSQLTOMYSQLConverter.Models;
    using RokonoDbManager.Models;

    public class MySQL : IAsyncDisposable
    {
        
        private List<BindingRowModel> _localData { get; set; }
        private SqlDataReader _reader {get; set;}
        List<OutboundTableConnection> _foreginKeys {get; set;}
        string _tableName {get; set;}
        string _primaryAutoInc {get; set;}
        public MySQL(List<BindingRowModel> data, SqlDataReader reader,  List<OutboundTableConnection> keys, string tableName, string primaryAutoInc)
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
                        DataType = $"INT AUTO_INCREMENT PRIMARY KEY",
                        IsNull = notNull
                    });
                else if(_foreginKeys.Any(x=>x.TableName == _tableName && x.ConnectionName == _reader.GetString(0)))
                    _localData.Add(new BindingRowModel{
                        TableName = _reader.GetString(0),
                        DataType = $"INT AUTO_INCREMENT PRIMARY KEY",
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
                    if(valueLenght > 255)
                        res =  DetermineType("varchar",valueLenght);
                    else
                        res = $"CHAR{lenght}"; 
                break;
                case "varchar":
                    if(valueLenght > 65535)
                        res =  DetermineType("text", valueLenght);
                    else
                        res = $"VARCHAR{lenght}";
                break;
                case "text":
                    res = $"TEXT{lenght}";
                break;
                case "nchar":
                    res = $"VARCHAR{lenght}";
                break;
                case "nvarchar":
                    if(valueLenght > 65535 && valueLenght != -1)
                        DetermineType("text", valueLenght);
                    else
                        res = $"VARCHAR{lenght}";
                break;  
                case "ntext":
                    res = $"LONGTEXT{lenght}";
                break;
                case "binary":
                    if(valueLenght > 65.535 && valueLenght != -1)
                        res = $"MEDIUMBLOB{lenght}";
                    else if(valueLenght >   16777215 && valueLenght != -1)
                        res = $"LONGBLOB{lenght}"; 
                    

                break;
                case "varbinary":
                       if(valueLenght > 65.535 && valueLenght != -1)
                        res = $"MEDIUMBLOB{lenght}";
                    else if(valueLenght >   16777215 && valueLenght != -1)
                        res = $"LONGBLOB{lenght}"; 
                break;
                case "varbinary(max)":
                       if(valueLenght > 65.535 && valueLenght != -1)
                        res = $"MEDIUMBLOB{lenght}";
                    else if(valueLenght >   16777215 && valueLenght != -1)
                        res = $"LONGBLOB{lenght}"; 
                break;
                case "image":
                       if(valueLenght > 65.535 && valueLenght != -1)
                        res = $"MEDIUMBLOB{lenght}";
                    else if(valueLenght > 16777215 && valueLenght != -1)
                        res = $"LONGBLOB{lenght}"; 
                break;
                case "bit":
                    res = $"CHAR"; 
                break;
                case "tinyint":
                    res = $"TINYINT{lenght}";
                break;
                case "smallint":
                    res = $"INT{lenght}";
                break;
                case "int":
                    res = $"INT{lenght}";
                break;
                case "bigint":
                    res = $"BIGINT{lenght}";
                break;
                case "decimal":
                    res =  $"DECIMAL{lenght}";
                break;
                case "numeric":
                    res = $"BIGINT{lenght}";

                break;
                case "smallmoney":
                    res = $"INT{lenght}";

                break;
                case "money":
                    res =  $"DECIMAL{lenght}";

                break;
                case "float":
                    res = $"FLOAT{lenght}";
                break;
                case "real":
                    res =  $"DECIMAL{lenght}";

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
                    res = "TIME";
                break;
                case "datetimeoffset":

                break;
                case "timestamp":
                    res = "TIMESTAMP";
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