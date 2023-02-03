call sir.bat truncate --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --collection wikipedia %*
call write.bat %*
call index.bat %*
call validate.bat %*