# &#9084; Resin Search Engine

[Overview](https://github.com/kreeben/resin/blob/master/README.md) | How to install | [User guide](https://github.com/kreeben/resin/blob/master/USER-GUIDE.md) 

## Installation of HTTP server

### Download
 
Download a [pre-built package for Windows x64](https://github.com/kreeben/resin/releases/download/v0.4.0.5/sir.httpserver.win-x64.zip) or:

`git clone https://github.com/kreeben/resin.git`

### Install

Extract zip file or, if you used `git clone`, now run the following commands:  

`cd resin\src\sir.httpserver`  

`dotnet publish -p:PublishProfile=FolderProfile`

### Launch server

`cd resin\src\sir.httpserver\bin\release\net5.0\publish`  

`dotnet run`

Your search server is now available at **http://localhost:54866**.

### How to use Sir.HttpServer

[Here](https://github.com/kreeben/resin/blob/master/src/Sir.HttpServer/README.md) are instructions for Sir.HttpServer.

## Installation of Sir.Cmd command-line utility

### Download
 
Download a [pre-built package for Windows x64](https://github.com/kreeben/resin/releases/download/v0.4.0.5/sir.cmd.win-x64.zip) or:

`git clone https://github.com/kreeben/resin.git`

### Install

Extract zip file or, if you used `git clone`, now run the following commands:  

`cd resin\src\sir.cmd`  

`dotnet publish -p:PublishProfile=FolderProfile`

`cd ..\\..\\`

### How to use Sir.Cmd command-line utility

[Here](https://github.com/kreeben/resin/blob/master/src/Sir.Cmd/README.md) are instructions on the commands you may issue through Sir.Cmd.