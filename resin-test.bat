call resin.bat truncate --dir C:\data --collection wikipedia %*
call resin.bat truncate --dir C:\data --collection wikipedia.composed %*
call resin.bat lexicon --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 100 --debug true %*
call resin.bat validatelexicon --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 100 --debug true %*
call resin.bat compose --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 100 --debug true %*
call resin.bat validatecomposed --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 100 --debug true %*