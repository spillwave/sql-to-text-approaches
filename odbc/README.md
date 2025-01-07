# ODBC on Mac is Dumb
But lets make it easier.

The docker-compose.yml file starts up the sql server db.

## Install ODBC Driver
From here you need to do some brew business. 

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18
```

## Configure ODBC Driver
And move the .odbc.ini file to your home directory

```shell
cp odbc/.odbc.ini ~/
```

## Validate ODBC Driver
Check ODBC
```shell
./bin/check_odbc.sh "YourStrong@Passw0rd"
```

or 

```shell
echo "SELECT name FROM sys.databases" | isql -v sqlserver sa YourStrong@Passw0rd
```

If that works then SQL server is running.  You can proceed with the ingestion script.
