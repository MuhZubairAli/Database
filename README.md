# Database Module for Android
  This is the Simplest ORM for Android which has support for API >= 17. It supports automatic Local Database creation from the Models list and has the utility to generate TSQL scripts for databases from given models. Currently, it supports CRUD operations for the Database.
  
It makes use of the following Annotations
  - PrimaryKey (autogenerate = false): for local database
  - SqlPrimaryKey (autogenerate = false, seed = 1, increment = 1) : for TSQL script
  - UniqueKey (index = default): both local database and TSQL script
  - SqlExclude: for TSQL
  - Default(value = ""): for both local database and TSQL
  - SqlDataType(value = ""): for TSQL special datatypes
  - NotNull: for both local database and TSQL
  - Table(name = "", version = 1): for both local database and TSQL
  - SerializedName(value="") for both local database and TSQL (This annotation is from GSON library for setting different name)

## Support for ModelBasedFactory
  This module also supports ModelBasedFactory Design Pattern for Database with inbuilt ExecutorService and all necessary helper functions for CRUD operations.
  
### Note
Multiple unique keys can be applied with different indexes. TSQL query for composite Primary Key works as expected with default ASC order for each column. But SQLite gives errors on the composite primary key.

## Todo
  - [x] Add support for a custom query select statement by returning a List of HashMap(s) of resulting columns
  - [ ] Implement support for custom name property in Table annotation (table name in createTable, CRUD methods, etc)
  - [ ] Automatic consolidated version calculation for Database
  - [ ] Implement support for SerializedName(value="") for both local database and TSQL (This annotation is from GSON library for setting different name for field)
