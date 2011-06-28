class DbTasks
  class ActiveRecordDbConfig < DbTasks::DbConfig
    def initialize(configuration)
      @configuration = configuration
    end

    attr_reader :configuration

    def no_create?
      true == config_value("no_create", true)
    end

    def catalog_name
      config_value("database", false)
    end

    protected

    def config_value(config_param_name, allow_nil)
      value = self.configuration[config_param_name]
      raise "Unable to locate configuration value named #{config_param_name}" if !allow_nil && value.nil?
      value
    end
  end

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

    def instance_registry_key
      config_value("instance_registry_key", false)
    end
  end

  class ActiveRecordDbDriver < DbTasks::DbDriver
    def execute(sql, execute_in_control_database = false)
      current_database = nil
      if execute_in_control_database
        current_database = self.current_database
        select_database(nil)
      end
      ActiveRecord::Base.connection.execute(sql, nil)
      select_database(current_database) if execute_in_control_database
    end

    def insert_row(table_name, row)
      column_names = row.keys.collect { |column_name| quote_column_name(column_name) }
      value_list = row.values.collect { |value| quote_value(value).gsub('[^\]\\n', "\n").gsub('[^\]\\r', "\r") }
      execute("INSERT INTO #{table_name} (#{column_names.join(', ')}) VALUES (#{value_list.join(', ')})")
    end

    def select_rows(sql)
      #TODO: Currently does not return times correctly. This needs to be fixed for fixture dumping to work
      ActiveRecord::Base.connection.select_rows(sql, nil)
    end

    def column_names_for_table(table)
      ActiveRecord::Base.connection.columns(table).collect { |c| quote_column_name(c.name) }
    end

    def open(config, open_control_database, log_filename)
      require 'active_record'
      raise "Can not open database connection. Connection already open." if open?
      ActiveRecord::Base.colorize_logging = false
      connection_config = config.configuration.dup
      connection_config['database'] = self.control_database_name if open_control_database
      ActiveRecord::Base.establish_connection(connection_config)
      FileUtils.mkdir_p File.dirname(log_filename)
      ActiveRecord::Base.logger = Logger.new(File.open(log_filename, 'a'))
      ActiveRecord::Migration.verbose = ENV["VERBOSE"] ? ENV["VERBOSE"] == "true" : false
    end

    def close
      ActiveRecord::Base.connection.disconnect! if open?
    end

    protected

    def open?
      ActiveRecord::Base.connection && ActiveRecord::Base.connection.active? rescue false
    end

    def quote_column_name(column_name)
      ActiveRecord::Base.connection.quote_column_name(column_name)
    end

    def quote_value(value)
      ActiveRecord::Base.connection.quote(value)
    end
  end

  class MssqlDbDriver < ActiveRecordDbDriver
    def create_schema(schema_name)
      if ActiveRecord::Base.connection.select_all("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
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

      catalog_name = configuration.catalog_name
      if database_version.nil?
        db_filename = catalog_name
      else
        db_filename = "#{catalog_name}_#{database_version.gsub(/\./, '_')}"
      end
      base_data_path = configuration.data_path
      base_log_path = configuration.log_path

      db_def = base_data_path ? "ON PRIMARY (NAME = [#{db_filename}], FILENAME='#{base_data_path}#{"\\"}#{db_filename}.mdf')" : ""
      log_def = base_log_path ? "LOG ON (NAME = [#{db_filename}_LOG], FILENAME='#{base_log_path}#{"\\"}#{db_filename}.ldf')" : ""

      collation_def = database.collation ? "COLLATE #{database.collation}" : ""

      execute("CREATE DATABASE [#{catalog_name}] #{db_def} #{log_def} #{collation_def}")
      execute(<<SQL)
ALTER DATABASE [#{catalog_name}] SET CURSOR_DEFAULT LOCAL
ALTER DATABASE [#{catalog_name}] SET CURSOR_CLOSE_ON_COMMIT ON

ALTER DATABASE [#{catalog_name}] SET AUTO_CREATE_STATISTICS ON
ALTER DATABASE [#{catalog_name}] SET AUTO_UPDATE_STATISTICS ON
ALTER DATABASE [#{catalog_name}] SET AUTO_UPDATE_STATISTICS_ASYNC ON

ALTER DATABASE [#{catalog_name}] SET ANSI_NULL_DEFAULT ON
ALTER DATABASE [#{catalog_name}] SET ANSI_NULLS ON
ALTER DATABASE [#{catalog_name}] SET ANSI_PADDING ON
ALTER DATABASE [#{catalog_name}] SET ANSI_WARNINGS ON
ALTER DATABASE [#{catalog_name}] SET ARITHABORT ON
ALTER DATABASE [#{catalog_name}] SET CONCAT_NULL_YIELDS_NULL ON
ALTER DATABASE [#{catalog_name}] SET QUOTED_IDENTIFIER ON
-- NUMERIC_ROUNDABORT OFF is required for filtered indexes. The optimizer will also
-- not consider indexed views if the setting is not set.
ALTER DATABASE [#{catalog_name}] SET NUMERIC_ROUNDABORT OFF
ALTER DATABASE [#{catalog_name}] SET RECURSIVE_TRIGGERS ON

ALTER DATABASE [#{catalog_name}] SET RECOVERY SIMPLE
SQL
      select_database(catalog_name)
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
    ALTER DATABASE [#{configuration.catalog_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
SQL
      end

      execute(<<SQL)
  IF EXISTS
    ( SELECT *
      FROM  sys.master_files
      WHERE state = 0 AND db_name(database_id) = '#{configuration.catalog_name}')
    DROP DATABASE [#{configuration.catalog_name}]
SQL
    end

    def backup(database, configuration)
      sql = <<SQL
  DECLARE @BackupDir VARCHAR(400)
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{configuration.instance_registry_key}\\MSSQLServer',
    @value_name='BackupDirectory',
    @value=@BackupDir OUTPUT
  IF @BackupDir IS NULL RAISERROR ('Unable to locate BackupDirectory registry key', 16, 1) WITH SETERROR
  DECLARE @BackupName VARCHAR(500)
  SET @BackupName = @BackupDir + '\\#{configuration.catalog_name}.bak'

BACKUP DATABASE [#{configuration.catalog_name}] TO DISK = @BackupName
WITH FORMAT, INIT, NAME = N'POST_CI_BACKUP', SKIP, NOREWIND, NOUNLOAD, STATS = 10
SQL
      execute(sql)
    end

    def restore(database, configuration)
      sql = <<SQL
  DECLARE @TargetDatabase VARCHAR(400)
  DECLARE @SourceDatabase VARCHAR(400)
  SET @TargetDatabase = '#{configuration.catalog_name}'
  SET @SourceDatabase = '#{configuration.restore_from}'

  DECLARE @BackupFile VARCHAR(400)
  DECLARE @DataLogicalName VARCHAR(400)
  DECLARE @LogLogicalName VARCHAR(400)
  DECLARE @DataDir VARCHAR(400)
  DECLARE @LogDir VARCHAR(400)

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

  IF @@RowCount <> 1 RAISERROR ('Unable to locate backupset', 16, 1) WITH SETERROR

  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{configuration.instance_registry_key}\\MSSQLServer',
    @value_name='DefaultData',
    @value=@DataDir OUTPUT
  IF @DataDir IS NULL RAISERROR ('Unable to locate DefaultData registry key', 16, 1) WITH SETERROR
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{configuration.instance_registry_key}\\MSSQLServer',
    @value_name='DefaultLog',
    @value=@LogDir OUTPUT
  IF @LogDir IS NULL RAISERROR ('Unable to locate DefaultLog registry key', 16, 1) WITH SETERROR

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
      execute("ALTER DATABASE [#{configuration.catalog_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
      execute(sql)
    end

    def pre_table_import(imp, module_name, table)
      identity_insert_sql = get_identity_insert_sql(table, true)
      execute(identity_insert_sql) if identity_insert_sql
    end

    def post_table_import(imp, module_name, table)
      identity_insert_sql = get_identity_insert_sql(table, false)
      execute(identity_insert_sql) if identity_insert_sql
      if imp.reindex?
        DbTasks.info("Reindexing #{table}")
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
            DbTasks.info("Reindexing #{table}")
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
      ActiveRecord::Base.connection.select_values(sql)
    end

    def current_database
      ActiveRecord::Base.connection.select_value("SELECT DB_NAME()")
    end

    def control_database_name
      'msdb'
    end

    def select_database(database_name)
      if database_name.nil?
        ActiveRecord::Base.connection.execute "USE [msdb]"
      else
        ActiveRecord::Base.connection.execute "USE [#{database_name}]"
      end
    end
  end

  class PostgresDbDriver < ActiveRecordDbDriver
    def create_schema(schema_name)
      if ActiveRecord::Base.connection.select_all("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
        execute("CREATE SCHEMA \"#{schema_name}\"")
      end
    end

    def drop_schema(schema_name, tables)
      execute("DROP SCHEMA \"#{schema_name}\"")
    end

    def create_database(database, configuration)
      execute(<<SQL)
CREATE DATABASE #{configuration.catalog_name}
SQL
    end

    def drop(database, configuration)
      unless ActiveRecord::Base.connection.select_all("SELECT * FROM pg_catalog.pg_database WHERE datname = '#{configuration.catalog_name}'").empty?
        execute("DROP DATABASE #{configuration.catalog_name}")
      end
    end

    def backup(database, configuration)
      raise NotImplementedError
    end

    def restore(database, configuration)
      raise NotImplementedError
    end

    def pre_table_import(imp, module_name, table)
    end

    def post_table_import(imp, module_name, table)
    end

    def post_data_module_import(imp, module_name)
    end

    protected

    def current_database
      ActiveRecord::Base.connection.select_value("SELECT DB_NAME()")
    end

    def control_database_name
      'postgres'
    end
  end
end
