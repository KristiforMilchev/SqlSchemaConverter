using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using MSSQLTOMYSQLConverter;
using MSSQLTOMYSQLConverter.Data_Hanlders;
using rokono_cl.DatabaseHandlers;
using RokonoDbManager.Models;

namespace rokono_cl.Data_Hanlders
{
    public class DiagramHandlers
    {
        public static async Task GenerateSchema(SavedConnection parameter, string dbName, string dbFilePath, int databaseType)
        {
            using(var context = new DbManager($"Server={parameter.Host};Database={dbName};User ID={parameter.Username};Password='{parameter.Password}';"))
            {

                var tables = await context.GetTables();
                var dbCreationScript = string.Empty;
                if(Program.DataBackup)
                    dbCreationScript += "SET GLOBAL FOREIGN_KEY_CHECKS=0;";
                var tablesForeignKeys = context.GetTableForignKeys();
                
                tables.ForEach(async x=>{
                    if(Program.DataBackup)
                        dbCreationScript += context.GetTableRows(x);
                    var od = await context.GetTableData(x,tablesForeignKeys,databaseType);
                    System.Console.WriteLine(od.CreationgString);
                    dbCreationScript += $"{od.CreationgString}\r\n";
                });
                
                var corelationData = context.GetDbUmlData();
                dbCreationScript += corelationData;
                //d System.Windows.Forms.Clipboard.SetText(dbCreationScript);
                System.Console.WriteLine("Clipboard middleware xclip for Linux is required otherwise the application throws an exception!");
                if(Program.DataBackup)
                    dbCreationScript += "SET GLOBAL FOREIGN_KEY_CHECKS=1;";


                if(!File.Exists($"{dbName}.sql"))
                {
                    using(var file = File.Create($"{dbName}.sql"))
                    {
                        
                    }
                }
                File.WriteAllText($"{dbName}.sql",dbCreationScript);

                System.Console.WriteLine("Text has been saved to your clipboard, plase add it to your editor of choice. Don't froget to double check the generated data types if you like to change the defaults on your own!!!");
                System.Console.WriteLine($"Creatiion script {dbName}.sql has been created in the root directory of the application in case the clipboard functionality fails!!! Known bug on Ubuntu based systems clipboard fails");
                if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    WindowsCopy.SetText(dbCreationScript);
                if(RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                    MacOsCopy.SetText(dbCreationScript);
                if(RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    LinuxCopy.SetText(dbCreationScript);

                var tempFileName = Path.GetTempFileName();
               
            }
         }
    }
}