namespace MSSQLTOMYSQLConverter
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using rokono_cl.CLHandlers;
    using rokono_cl.Data_Hanlders;
    using RokonoDbManager.Models;

    class Program
    {
        public static SavedConnection SavedConnection {get; set;}
        public static string Password {get;set;}
        public static string User {get;set;}
        public static string Database { get; set; }
        public static string FilePath {get; set;}
        public static string Ip {get; set;}
        public static int ConversionType { get; set; }
        public static bool DataBackup { get; set; }
        static async Task Main(string[] args)
        {
            try 
            {
                for(int i = 0; i < args.Length; i++)
                {
                    switch(args[i])
                    {
                        case "-u":
                            User = args[i+1];
                            break;
                        case "-password":
                            Password = args[i+1];
                            break;
                        case "-d":
                            Database = args[i+1];
                            break;
                        case "-file":
                            FilePath = args[i+1];
                            break;  
                        case "-a":
                            Ip = args[i+1];
                        break;
                        case "-e":
                            InputHandler.EditConnection();
                        break;
                        case "-r":
                            InputHandler.RemoveConnection();
                        break;
                        case "-s":
                            InputHandler.SaveDatabaseGen();
                            break;
                        case "-L":
                            InputHandler.GetConnections();
                            break;
                        case "-b":
                            DataBackup = true;
                        break;
                        case "-CT":
                            ConversionType = int.Parse(args[i-1]);
                        break;
                        case "-Connection":
                            SavedConnection = InputHandler.GetConnectionById(args[i+1]);
                            System.Console.WriteLine(SavedConnection.ConnectionString);
                            break;
                        case "-Convert":
                            await InputHandler.ConvertDatabase(ConversionType);
                        break;
                        case "--Help":
                            ShowHelpMenu();
                            break;

                        
                    }  
                }
    
                if(args.Length == 0)
                    System.Console.WriteLine("Use rokono-cl --Help  *for more information*");

            }
            catch(Exception ex)
            {
                System.Console.WriteLine("Invalid paramter input!!!Please follow the instructiosn in --Help");
                System.Console.WriteLine(ex.ToString());
                throw;
            }            
        }

       

        private static void ShowHelpMenu()
        {
            System.Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
            System.Console.WriteLine("Name: MSSQLTOMYSQLCONVETER");
            System.Console.WriteLine("Description: Quick and easy tool to generate UML diagrams from relational databases released as an extension of plantUML extension for visual studio code");
            System.Console.WriteLine("The tool comes as is and its not supported or developed by the team behind plantUML, but it is fully integrated to work with the drawing libaray to generate database UML diagrams.");
            System.Console.WriteLine("Author: Kristifor Milchev");
            System.Console.WriteLine("Contact for support: Kristifor@rttinternational.com");
            System.Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
            System.Console.WriteLine();
            System.Console.WriteLine();
            System.Console.WriteLine("Usage example MSSQLTOMYSQLCONVETER [options] [commands]");
            System.Console.WriteLine("-u : Prompts for a username it should be an account that has access to view table relationships. ");
            System.Console.WriteLine("-password: requires a valid password for the current sql user");
            System.Console.WriteLine("-d: the default database that will be used  ");
            System.Console.WriteLine("-a: the endpoint of the sql server, it could be a domain or an ip and if it doesn't run on the default port please specify it with ip:port");
            System.Console.WriteLine("-L: returns a list of saved database diagrams for quick access.");
            System.Console.WriteLine("-r: removes a record from the saved connections");
            System.Console.WriteLine("-e: edits a saved connection");
            System.Console.WriteLine("-Connection: requires specified Id after the command in order to select a connection from the quick access list. Saved quick connectison can be viewd with -L for identified use the ID column result");
            System.Console.WriteLine("-----------------------------------------------------------------------------------------------------------");
            System.Console.WriteLine("                              Please include the commnds after the supplied options                         ");
            System.Console.WriteLine("-----------------------------------------------------------------------------------------------------------");

            System.Console.WriteLine("-s: specify this command at the end of the new connection in order to save it for quick access in the future. Example rokono-cl -u User -password \"Password\" -a ip -d DatabaseName -s ");
            System.Console.WriteLine("-e: specify this command at the end of the new connection followed by -Connection ID in order to edit a record in the saved connections list.");
            System.Console.WriteLine("-r: specitfy this command after -Connection ID in order to remove a connection from the saved connections list.");
            System.Console.WriteLine("-CT: Specify this command in order to set the conversion type with the following options coresponding:");
            System.Console.WriteLine("-CT 1 -> MySQL");
            System.Console.WriteLine("-CT 2 -> SQLITE");
            System.Console.WriteLine("-Convert:Executes the queries needed to convert an existing MSSQL database to MySQL database, returns a text  creation script in the clipboard and a file in the main excution directory. Linux has known issue the clipboard doesn't wok!!!");
            System.Console.WriteLine("-----------------------------------------------------------------------------------------------------------");

        }
    }
}