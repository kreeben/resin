# Setup Wikipedia Locally

This project includes a command-line helper to download a Wikipedia CirrusSearch dump and then run the build/validate lexicon operations locally.

## 1) Download a CirrusSearch Dump
Use the `wikipediadownload` command to fetch a specific dump. The URL follows the pattern used by Wikimedia dumps.

**Examples:**
- Download English Wikipedia (enwiki), November 22, 2021 content dump:
  - `resin.bat wikipediadownload --dest "d:\enwiki" --project enwiki --date 20211122 --type content`
- Download English Wikipedia search dump for another date (replace the date):
  - `resin.bat wikipediadownload --dest "d:\enwiki" --project enwiki --date YYYYMMDD --type content`

**Notes:**
- The file will be saved to `d:\enwiki\enwiki-<date>-cirrussearch-content.json.gz`.
- Dates must match an available dump. See [Wikimedia Dumps](https://dumps.wikimedia.org/other/cirrussearch/) for available dates.

## 2) Build and Validate the Lexicon
Once the file is downloaded, run the following commands:

- **Build Lexicon:**
  - `resin.bat lexicon --dir "c:\data" --source "d:\enwiki\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 100 --debug true`
  
- **Validate Lexicon:**
  - `resin.bat validatelexicon --dir "c:\data" --source "d:\enwiki\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 100 --debug true`

**Optional:**
- Truncate storage before rebuilding:
  - `resin.bat truncate --dir c:\data --collection wikipedia`
  - `resin.bat truncate --dir c:\data --collection wikipedia.composed`

# Additional Information
For further details on usage, configuration, and troubleshooting, please refer to the documentation or contact the project maintainers.

# License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

# Acknowledgments
Thanks to all contributors and the Wikimedia community for providing the data and resources necessary for this project.