class DbTasks
  class MssqlDbConfig < ActiveRecordDbConfig

    def force_drop?
      true == config_value("force_drop", true)
    end

    def data_path
      config_value("data_path", true)
    end

    def log_path
      config_value("log_path", true)
    end

    def restore_from
      config_value("restore_from", false)
    end

    def backup_location
      config_value("backup_location", true)
    end

    def instance_registry_key
      config_value("instance_registry_key", false)
    end
  end

  class MssqlDbDriver < ActiveRecordDbDriver
    def create_schema(schema_name)
      if select_rows("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
        execute("CREATE SCHEMA [#{schema_name}]")
      end
    end

    def drop_schema(schema_name, tables)
      #TODO: A better dependency list is as follows
=begin
WITH TablesCTE(SchemaName, Name, type, Ordinal, object_id) AS
(
    SELECT
        OBJECT_SCHEMA_NAME(so.object_id),
        OBJECT_NAME(so.object_id),
        so.type,
        0 AS Ordinal,
        so.object_id
    FROM
        sys.objects AS so
    WHERE
        so.is_ms_Shipped = 0
    UNION ALL
    SELECT
        OBJECT_SCHEMA_NAME(so.object_id),
        OBJECT_NAME(so.object_id),
        so.type,
        tt.Ordinal + 1 AS Ordinal,
        so.object_id
    FROM
        sys.objects AS so
    JOIN sys.foreign_keys AS f
        ON f.parent_object_id = so.object_id AND f.parent_object_id != f.referenced_object_id
    JOIN TablesCTE AS tt
        ON f.referenced_object_id = tt.object_id
    WHERE
        so.is_ms_Shipped = 0
)
SELECT DISTINCT
        t.Ordinal,
        t.SchemaName,
        t.Name,
        t.type
    FROM
        TablesCTE AS t
    JOIN
        (
            SELECT
                itt.SchemaName,
                itt.Name,
                itt.type,
                Max(itt.Ordinal) as Ordinal
            FROM
                TablesCTE AS itt
            GROUP BY
                itt.SchemaName,
                itt.Name,
                itt.type
        ) AS tt
        ON t.SchemaName = tt.SchemaName AND t.Name = tt.Name AND t.type = tt.type AND t.Ordinal = tt.Ordinal
WHERE t.type IN (
  'FN', -- SQL scalar function
  'IF', -- SQL inline table-valued function
  'P', -- SQL Stored Procedure
  'TF', -- SQL table-valued-function
  'TR', -- SQL DML trigger
  'U', -- Table (user-defined)
  'V' -- View
)
ORDER BY t.Ordinal, t.Name
=end

      database_objects("SQL_STORED_PROCEDURE", schema_name).each { |name| execute("DROP PROCEDURE #{name}") }
      database_objects("SQL_SCALAR_FUNCTION", schema_name).each { |name| execute("DROP FUNCTION #{name}") }
      database_objects("SQL_INLINE_TABLE_VALUED_FUNCTION", schema_name).each { |name| execute("DROP FUNCTION #{name}") }
      database_objects("SQL_TABLE_VALUED_FUNCTION", schema_name).each { |name| execute("DROP FUNCTION #{name}") }
      database_objects("VIEW", schema_name).each { |name| execute("DROP VIEW #{name}") }
      tables.reverse.each do |table|
        execute("DROP TABLE #{table}")
      end
      execute("DROP SCHEMA #{schema_name}")
    end

    def create_database(database, configuration)
      database_version = database.version

      if database_version.nil?
        db_filename = configuration.catalog_name
      else
        db_filename = "#{configuration.catalog_name}_#{database_version.gsub(/\./, '_')}"
      end
      base_data_path = configuration.data_path
      base_log_path = configuration.log_path

      db_def = base_data_path ? "ON PRIMARY (NAME = [#{db_filename}], FILENAME='#{base_data_path}#{"\\"}#{db_filename}.mdf')" : ""
      log_def = base_log_path ? "LOG ON (NAME = [#{db_filename}_LOG], FILENAME='#{base_log_path}#{"\\"}#{db_filename}.ldf')" : ""

      collation_def = database.collation ? "COLLATE #{database.collation}" : ""

      quoted_catalog_name = quote_table_name(configuration.catalog_name)
      execute("CREATE DATABASE #{quoted_catalog_name} #{db_def} #{log_def} #{collation_def}")
      execute(<<SQL)
ALTER DATABASE #{quoted_catalog_name} SET CURSOR_DEFAULT LOCAL
ALTER DATABASE #{quoted_catalog_name} SET CURSOR_CLOSE_ON_COMMIT ON

ALTER DATABASE #{quoted_catalog_name} SET AUTO_CREATE_STATISTICS ON
ALTER DATABASE #{quoted_catalog_name} SET AUTO_UPDATE_STATISTICS ON
ALTER DATABASE #{quoted_catalog_name} SET AUTO_UPDATE_STATISTICS_ASYNC ON

ALTER DATABASE #{quoted_catalog_name} SET ANSI_NULL_DEFAULT ON
ALTER DATABASE #{quoted_catalog_name} SET ANSI_NULLS ON
ALTER DATABASE #{quoted_catalog_name} SET ANSI_PADDING ON
ALTER DATABASE #{quoted_catalog_name} SET ANSI_WARNINGS ON
ALTER DATABASE #{quoted_catalog_name} SET ARITHABORT ON
ALTER DATABASE #{quoted_catalog_name} SET CONCAT_NULL_YIELDS_NULL ON
ALTER DATABASE #{quoted_catalog_name} SET QUOTED_IDENTIFIER ON
-- NUMERIC_ROUNDABORT OFF is required for filtered indexes. The optimizer will also
-- not consider indexed views if the setting is not set.
ALTER DATABASE #{quoted_catalog_name} SET NUMERIC_ROUNDABORT OFF
ALTER DATABASE #{quoted_catalog_name} SET RECURSIVE_TRIGGERS ON

ALTER DATABASE #{quoted_catalog_name} SET RECOVERY SIMPLE
SQL
      select_database(configuration.catalog_name)
      unless database_version.nil?
        execute("EXEC sys.sp_addextendedproperty @name = N'DatabaseSchemaVersion', @value = N'#{database_version}'")
      end
    end

    def drop(database, configuration)
      if configuration.force_drop?
        execute(<<SQL)
  IF EXISTS
    ( SELECT *
      FROM  sys.master_files
      WHERE state = 0 AND db_name(database_id) = '#{configuration.catalog_name}')
    ALTER DATABASE #{quote_table_name(configuration.catalog_name)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE
SQL
      end

      execute(<<SQL)
  IF EXISTS
    ( SELECT *
      FROM  sys.master_files
      WHERE state = 0 AND db_name(database_id) = '#{configuration.catalog_name}')
    DROP DATABASE #{quote_table_name(configuration.catalog_name)}
SQL
    end

    def backup(database, configuration)
      sql = "DECLARE @BackupName VARCHAR(500)"
      if configuration.backup_location
        sql << "SET @BackupName = '#{configuration.backup_location}'"
      else
        sql << <<SQL
  DECLARE @BackupDir VARCHAR(400)
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{configuration.instance_registry_key}\\MSSQLServer',
    @value_name='BackupDirectory',
    @value=@BackupDir OUTPUT
  IF @BackupDir IS NULL RAISERROR ('Unable to locate BackupDirectory registry key', 16, 1) WITH SETERROR
  SET @BackupName = @BackupDir + '\\#{configuration.catalog_name}.bak'
SQL
      end

      sql << <<SQL
  BACKUP DATABASE #{quote_table_name(configuration.catalog_name)} TO DISK = @BackupName
  WITH FORMAT, INIT, NAME = N'POST_CI_BACKUP', SKIP, NOREWIND, NOUNLOAD, STATS = 10
SQL
      execute(sql)
    end

    def restore(database, configuration)
      execute(<<SQL)
  IF EXISTS (SELECT * FROM sys.databases WHERE name = '#{configuration.catalog_name}')
    ALTER DATABASE #{configuration.catalog_name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

  DECLARE @TargetDatabase VARCHAR(400)
  DECLARE @SourceDatabase VARCHAR(400)
  SET @TargetDatabase = '#{configuration.catalog_name}'
  SET @SourceDatabase = '#{configuration.restore_from}'

  DECLARE @BackupFile VARCHAR(400)
  DECLARE @DataLogicalName VARCHAR(400)
  DECLARE @LogLogicalName VARCHAR(400)
  DECLARE @DataDir VARCHAR(400)
  DECLARE @LogDir VARCHAR(400)
  DECLARE @DataRoot VARCHAR(400)

#{get_backup_file_list_locations(configuration)}

  IF @@RowCount <> 1 RAISERROR ('Unable to locate backupset', 16, 1) WITH SETERROR

  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{configuration.instance_registry_key}\\Setup',
    @value_name='SQLDataRoot',
    @value=@DataRoot OUTPUT

  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{configuration.instance_registry_key}\\MSSQLServer',
    @value_name='DefaultData',
    @value=@DataDir OUTPUT

  IF @DataDir IS NULL AND @DataRoot IS NOT NULL
    SET @DataDir = @DataRoot + '\\Data'
  IF @DataDir IS NULL RAISERROR ('Unable to locate DefaultData or SQLDataRoot registry key', 16, 1) WITH SETERROR

  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{configuration.instance_registry_key}\\MSSQLServer',
    @value_name='DefaultLog',
    @value=@LogDir OUTPUT
  IF @LogDir IS NULL AND @DataRoot IS NOT NULL
    SET @LogDir = @DataRoot + '\\Data'
  IF @LogDir IS NULL RAISERROR ('Unable to locate DefaultLog or SQLDataRoot registry key', 16, 1) WITH SETERROR

  DECLARE @sql VARCHAR(4000)
  SET @sql = 'RESTORE DATABASE [' + @TargetDatabase + '] FROM DISK = N''' + @BackupFile + ''' WITH  FILE = 1,
  MOVE N''' + @DataLogicalName + ''' TO N''' + @DataDir + '\\' + @TargetDatabase + '.MDF'',
  MOVE N''' + @LogLogicalName + ''' TO N''' + @LogDir + '\\' + @TargetDatabase + '.LDF'',
  NOUNLOAD,
  REPLACE,
  STATS = 10
  '
  EXEC(@sql)
SQL
    end

    def pre_table_import(imp, module_name, table)
      identity_insert_sql = get_identity_insert_sql(table, true)
      execute(identity_insert_sql) if identity_insert_sql
    end

    def post_table_import(imp, module_name, table)
      identity_insert_sql = get_identity_insert_sql(table, false)
      execute(identity_insert_sql) if identity_insert_sql
      if imp.reindex?
        DbTasks.info("Reindexing #{DbTasks.clean_table_name(table)}")
        execute("DBCC DBREINDEX (N'#{table}', '', 0) WITH NO_INFOMSGS")
      end
    end

    def post_data_module_import(imp, module_name)
      if imp.reindex?
        sql_prefix = "DECLARE @DbName VARCHAR(100); SET @DbName = DB_NAME();"

        if imp.shrink?
          # We are shrinking the database in case any of the import scripts created tables/columns and dropped them
          # later. This would leave large chunks of empty space in the underlying files. However it has to be done before
          # we reindex otherwise the indexes will be highly fragmented.
          DbTasks.info("Shrinking database")
          execute("#{sql_prefix} DBCC SHRINKDATABASE(@DbName, 10, NOTRUNCATE) WITH NO_INFOMSGS")
          execute("#{sql_prefix} DBCC SHRINKDATABASE(@DbName, 10, TRUNCATEONLY) WITH NO_INFOMSGS")

          imp.database.table_ordering(module_name).each do |table|
            DbTasks.info("Reindexing #{DbTasks.clean_table_name(table)}")
            execute("DBCC DBREINDEX (N'#{table}', '', 0) WITH NO_INFOMSGS")
          end
        end

        DbTasks.info("Updating statistics")
        execute("EXEC dbo.sp_updatestats")

        # This updates the usage details for the database. i.e. how much space is take for each index/table
        DbTasks.info("Updating usage statistics")
        execute("#{sql_prefix} DBCC UPDATEUSAGE(@DbName) WITH NO_INFOMSGS, COUNT_ROWS")
      end
    end

    protected

    def has_identity_column(table)
      ActiveRecord::Base.connection.columns(table).each do |c|
        return true if c.identity == true
      end
      false
    end

    def get_identity_insert_sql(table, value)
      if has_identity_column(table)
        "SET IDENTITY_INSERT #{table} #{value ? 'ON' : 'OFF'}"
      else
        nil
      end
    end

    def database_objects(object_type, schema_name)
      sql = <<SQL
SELECT QUOTENAME(S.name) + '.' + QUOTENAME(O.name)
FROM
sys.objects O
JOIN sys.schemas S ON O.schema_id = S.schema_id AND S.name = '#{schema_name}' AND O.parent_object_id = 0
WHERE type_desc = '#{object_type}'
ORDER BY create_date DESC
SQL
      select_values(sql)
    end

    def current_database
      select_value("SELECT DB_NAME()")
    end

    def control_database_name
      'msdb'
    end

    def select_database(database_name)
      if database_name.nil?
        execute("USE [msdb]")
      else
        execute("USE [#{database_name}]")
      end
    end

    def get_backup_file_list_locations(configuration)
      if configuration.backup_location
        <<SQL
  SET @BackupFile = '#{configuration.backup_location}\\#{configuration.restore_from}.bak';

  DECLARE @FileList TABLE
      (
      LogicalName NVARCHAR(128) NOT NULL,
      PhysicalName NVARCHAR(260) NOT NULL,
      Type CHAR(1) NOT NULL,
      FileGroupName NVARCHAR(120) NULL,
      Size NUMERIC(20, 0) NOT NULL,
      MaxSize NUMERIC(20, 0) NOT NULL,
      FileID BIGINT NULL,
      CreateLSN NUMERIC(25,0) NULL,
      DropLSN NUMERIC(25,0) NULL,
      UniqueID UNIQUEIDENTIFIER NULL,
      ReadOnlyLSN NUMERIC(25,0) NULL ,
      ReadWriteLSN NUMERIC(25,0) NULL,
      BackupSizeInBytes BIGINT NULL,
      SourceBlockSize INT NULL,
      FileGroupID INT NULL,
      LogGroupGUID UNIQUEIDENTIFIER NULL,
      DifferentialBaseLSN NUMERIC(25,0)NULL,
      DifferentialBaseGUID UNIQUEIDENTIFIER NULL,
      IsReadOnly BIT NULL,
      IsPresent BIT NULL,
      TDEThumbprint VARBINARY(32) NULL
   );

   INSERT INTO @FileList EXEC('RESTORE FILELISTONLY FROM DISK = ''' + @BackupFile + '''')
   SELECT TOP 1 @DataLogicalName = LogicalName FROM @FileList WHERE Type = 'D' ORDER BY DifferentialBaseLSN
   SELECT TOP 1 @LogLogicalName = LogicalName FROM @FileList WHERE Type = 'L' ORDER BY DifferentialBaseLSN
SQL
      else
        <<SQL
  SELECT
  @BackupFile = MF.physical_device_name, @DataLogicalName = BF_Data.logical_name, @LogLogicalName = BF_Log.logical_name
  FROM
    msdb.dbo.backupset BS
  JOIN msdb.dbo.backupmediafamily MF ON MF.media_set_id = BS.media_set_id
  JOIN msdb.dbo.backupfile BF_Data ON BF_Data.backup_set_id = BS.backup_set_id AND BF_Data.file_type = 'D'
  JOIN msdb.dbo.backupfile BF_Log ON BF_Log.backup_set_id = BS.backup_set_id AND BF_Log.file_type = 'L'
  WHERE
    BS.backup_set_id =
    (
      SELECT TOP 1 backup_set_id
      FROM msdb.dbo.backupset
      WHERE database_name = @SourceDatabase
      ORDER BY backup_start_date DESC
    )
SQL
      end
    end
  end
end
