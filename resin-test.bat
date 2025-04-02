call resin.bat truncate --dir C:\data --collection wikipedia %*
call resin.bat analyze --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 10 %*
call resin.bat validate --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 10 %*