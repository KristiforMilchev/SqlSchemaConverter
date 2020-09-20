using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MSSQLTOMYSQLConverter;
using Newtonsoft.Json;
using rokono_cl.Data_Hanlders;
using rokono_cl.DatabaseHandlers;
using RokonoDbManager.Models;

namespace rokono_cl.CLHandlers
{
    public class InputHandler
    {
        public static List<SavedConnection> GetSavedConnections()
        {
            if(!File.Exists("SavedConnections.txt"))
            {
                System.Console.WriteLine("Empty");
                return new List<SavedConnection>();
            }
            var fileData = File.ReadAllText("SavedConnections.txt");
            return JsonConvert.DeserializeObject<List<SavedConnection>>(fileData);   
        }

        internal static void SavedConnections(List<SavedConnection> getCons)
        {
            
            if(!File.Exists("SavedConnections.txt"))
            {
                File.Create("SavedConnections.txt").Close();
                AddConnections(getCons,"SavedConnections.txt");
            }
            else
                AddConnections(getCons,"SavedConnections.txt");
             
        }

        internal static void AddConnections(List<SavedConnection> getCons, string v)
        {
            File.WriteAllText(v,JsonConvert.SerializeObject(getCons));
        }

         

        internal static SavedConnection GetSavedConnection(string conId)
        {

            if(!File.Exists("SavedConnections.txt"))
                return null;
            var fileData = File.ReadAllText("SavedConnections.txt");
            return JsonConvert.DeserializeObject<List<SavedConnection>>(fileData).FirstOrDefault(x=>x.ConnectionId == int.Parse(conId));        
        } 


        internal static async Task ConvertDatabase(int conversionType) 
        {
            if(Program.SavedConnection == null)
                Program.SavedConnection = new SavedConnection{
                    Database = Program.Database,
                    Host = Program.Ip,
                    Password = Program.Password,
                    Username = Program.User,
                };
             await DiagramHandlers.GenerateSchema(Program.SavedConnection, Program.SavedConnection.Database,
                                           Program.SavedConnection.FilePath, Program.SavedConnection.DbType);
        }  
     

        internal static SavedConnection GetConnectionById(string conId)
        {
            var result = InputHandler.GetSavedConnection(conId);
            if(result == null)
            {
                System.Console.WriteLine("Connection doesn't exist please try connecting to the database using the following syntax. ");
                System.Console.WriteLine("-U username -password password -d databasename -file filepath -a hostip -s to save for later use");
                System.Console.WriteLine("You can also do --help for more information");
            }
            return result;
        }
        internal static void GetConnections()
        {
            var getCons = InputHandler.GetSavedConnections();
            Console.WriteLine(getCons.ToStringTable(
                new[] {"ID", "Database Name", "Host", "File Path", "Context Path"},
                a => a.ConnectionId, a => a.Database, a => a.Host, a=> a.FilePath, a=> a.DbContextPath));
        }
        
        internal static void SaveDatabaseGen()
        {
            System.Console.WriteLine("In");
            var data = InputHandler.GetSavedConnections();
            var getCons = data == null ? new List<SavedConnection>() : data;
            var count =  getCons.Count == 0 ?  getCons.Count + 1 : getCons.Count + 1;
            var conStirng =$"Server={Program.Ip};Database={Program.Database};User ID={Program.User};Password='{Program.Password}';";
            var savedConnection = new SavedConnection{
                Username = Program.User,
                Password = Program.Password,
                FilePath = Program.FilePath,
                Database = Program.Database,
                Host = Program.Ip,
                ConnectionString = conStirng,
                ConnectionId = count,
             };

            getCons.Add(savedConnection);
            InputHandler.SavedConnections(getCons);
        }   
        internal static void EditConnection()
        {
            System.Console.WriteLine("In");
            var data = InputHandler.GetSavedConnections();
            var getCons = data == null ? new List<SavedConnection>() : data;
            
            var conStirng =$"Server={Program.Ip};Database={Program.Database};User ID={Program.User};Password='{Program.Password}';";
           
            Program.SavedConnection.Username = Program.User;
            Program.SavedConnection.Password = Program.Password;
            Program.SavedConnection.FilePath = Program.FilePath;
            Program.SavedConnection.Database = Program.Database;
            Program.SavedConnection.Host = Program.Ip;
            Program.SavedConnection.ConnectionString = conStirng;
         
            getCons.Add(Program.SavedConnection);
            InputHandler.SavedConnections(getCons);
        }
        internal static void RemoveConnection()
        {
            var data = InputHandler.GetSavedConnections();
            var getCons = data == null ? new List<SavedConnection>() : data;
            if(getCons.Count == 0)
            {
                System.Console.WriteLine("Collection is empty, you can't delete an non existing row!");
            }
            
            getCons.Remove(Program.SavedConnection);
            var rebase = new List<SavedConnection>();
            getCons.ForEach(x=>{
                var current = x;
                current.ConnectionId = current.ConnectionId -1;
                rebase.Add(current);
            });
            InputHandler.SavedConnections(getCons);

        }
    }
}