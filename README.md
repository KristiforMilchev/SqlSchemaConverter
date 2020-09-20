# SqlSchemaConverter



 is a simple tool that converts MSSQL databases to MySql it was created under a day and its main purpose was to transfer few big databases between servers, the code behind is a mess but when i get time i will fix it, for now it works

##List of commands:

Usage example SqlSchemaConverter [options] [commands] -u : Prompts for a username it should be an account that has access to view table relationships. -password: requires a valid password for the current sql user -d: the default database that will be used
-a: the endpoint of the sql server, it could be a domain or an ip and if it doesn't run on the default port please specify it with ip:port -L: returns a list of saved database diagrams for quick access. -r: removes a record from the saved connections -e: edits a saved connection -Connection: requires specified Id after the command in order to select a connection from the quick access list. Saved quick connectison can be viewd with -L for identified use the -s: specify this command at the end of the new connection in order to save it for quick access in the future. Example rokono-cl -u User -password "Password" -a ip -d DatabaseName -s -e: specify this command at the end of the new connection followed by -Connection ID in order to edit a record in the saved connections list. -r: specitfy this command after -Connection ID in order to remove a connection from the saved connections list. -Convert:Executes the queries needed to convert an existing MSSQL database to MySQL database, returns a text creation script in the clipboard and a file in the main excution directory. Linux has known issue the clipboard doesn't wok!!!"

To compile the program make sure that you have the latest vesion of .net core 3.1

Execute the coresponding command for the operating system that you're building. For example if you're trying to build under linux use dotnet publish -c Release -r linux-x64

#Licensed under MIT.
