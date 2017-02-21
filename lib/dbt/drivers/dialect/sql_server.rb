#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

class Dbt
  module SqlServerConfig

    def build_jdbc_url(options = {})
      credentials_inline = options[:credentials_inline].nil? ? false : options[:credentials_inline]
      use_control_catalog = options[:use_control_catalog].nil? ? false : options[:use_control_catalog]

      url = "jdbc:jtds:sqlserver://#{host}:#{port}/"
      url += use_control_catalog ? control_catalog_name : catalog_name
      url += ";instance=#{instance}" if instance
      url += ";appname=#{appname}" if appname
      if credentials_inline
        url += ";user=#{username}"
        url += ";password=#{password}"
      end
      url
    end

    def control_catalog_name
      'msdb'
    end

    def port
      @port || 1433
    end

    attr_accessor :instance
    attr_accessor :appname
    attr_accessor :data_path
    attr_accessor :log_path
    # Short key used to generate filename restored from.
    # Something like "MYDATABASE" results in restore from backup "MYDATABASE.bak".
    attr_accessor :restore_name
    # Short key used to generate filename backed up to. S
    # Something like "MYDATABASE" results in backup to file "MYDATABASE.bak".
    attr_accessor :backup_name
    attr_accessor :backup_location
    attr_writer :force_drop

    def force_drop?
      !!@force_drop
    end

    attr_writer :delete_backup_history

    def delete_backup_history?
      @delete_backup_history.nil? ? true : @delete_backup_history
    end

    attr_writer :reindex_on_import

    def reindex_on_import?
      @reindex_on_import.nil? ? true : @reindex_on_import
    end

    attr_writer :shrink_on_import

    def shrink_on_import?
      @shrink_on_import.nil? ? false : @shrink_on_import
    end
  end

  module Dialect
    module SqlServer

      def update_sequence(sequence_name, value)
        execute("ALTER SEQUENCE #{sequence_name} RESTART WITH #{value}")
      end

      def create_schema(schema_name)
        if query("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
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

        database_objects('SQL_STORED_PROCEDURE', schema_name).each { |name| execute("DROP PROCEDURE #{name}") }
        database_objects('SQL_SCALAR_FUNCTION', schema_name).each { |name| execute("DROP FUNCTION #{name}") }
        database_objects('SQL_INLINE_TABLE_VALUED_FUNCTION', schema_name).each { |name| execute("DROP FUNCTION #{name}") }
        database_objects('SQL_TABLE_VALUED_FUNCTION', schema_name).each { |name| execute("DROP FUNCTION #{name}") }
        database_objects('VIEW', schema_name).each { |name| execute("DROP VIEW #{name}") }
        existing = database_objects('USER_TABLE', schema_name)
        tables.each do |table|
          execute("DROP TABLE #{table}") if existing.include?(table)
        end
        database_objects('SEQUENCE_OBJECT', schema_name).each { |name| execute("DROP SEQUENCE #{name}") }
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

        quoted_catalog_name = quote_table_name(configuration.catalog_name)
        execute("CREATE DATABASE #{quoted_catalog_name} #{db_def} #{log_def}")
        select_database(configuration.catalog_name)
        unless database_version.nil?
          execute("EXEC sys.sp_addextendedproperty @name = N'DatabaseSchemaVersion', @value = N'#{database_version}'")
        end
      end

      def drop(database, configuration)
        execute('SET DEADLOCK_PRIORITY HIGH')
        execute("EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'#{configuration.catalog_name}'") if configuration.delete_backup_history?
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
        sql = <<SQL
  DECLARE @RegKey VARCHAR(400)
#{get_instance_key_sql}
#{get_backup_name_sql(configuration, configuration.backup_name || Dbt::Naming.uppercase_constantize(database.key.to_s))}
  BACKUP DATABASE #{quote_table_name(configuration.catalog_name)} TO DISK = @BackupName
  WITH FORMAT, INIT, NAME = N'POST_CI_BACKUP', SKIP, NOREWIND, NOUNLOAD, STATS = 10
SQL
        execute(sql)
      end

      def restore(database, configuration)
        execute(<<SQL)
  IF EXISTS (SELECT * FROM sys.databases WHERE name = '#{configuration.catalog_name}')
  ALTER DATABASE #{quote_table_name(configuration.catalog_name)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DECLARE @RegKey VARCHAR(400)
#{get_instance_key_sql}
#{get_backup_name_sql(configuration, configuration.restore_name || Dbt::Naming.uppercase_constantize(database.key.to_s))}
  DECLARE @DataLogicalName VARCHAR(400)
  DECLARE @LogLogicalName VARCHAR(400)
  DECLARE @DataDir VARCHAR(400)
  DECLARE @LogDir VARCHAR(400)
  DECLARE @DataRoot VARCHAR(400)

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
     ReadOnlyLSN NUMERIC(25,0) NULL,
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

  INSERT INTO @FileList EXEC('RESTORE FILELISTONLY FROM DISK = ''' + @BackupName + '''')
  SELECT TOP 1 @DataLogicalName = LogicalName FROM @FileList WHERE Type = 'D' ORDER BY DifferentialBaseLSN
  SELECT TOP 1 @LogLogicalName = LogicalName FROM @FileList WHERE Type = 'L' ORDER BY DifferentialBaseLSN

  IF @@RowCount <> 1 RAISERROR ('Unable to locate backupset', 16, 1) WITH SETERROR

  SET @RegKey = 'SOFTWARE\\Microsoft\\Microsoft SQL Server\\' + @InstanceKey + '\\Setup'
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE', @key=@RegKey, @value_name='SQLDataRoot', @value=@DataRoot OUTPUT

  SET @RegKey = 'SOFTWARE\\Microsoft\\Microsoft SQL Server\\' + @InstanceKey + '\\MSSQLServer'
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE', @key=@RegKey, @value_name='DefaultData', @value=@DataDir OUTPUT

  IF @DataDir IS NULL AND @DataRoot IS NOT NULL SET @DataDir = @DataRoot + '\\Data'
  IF @DataDir IS NULL RAISERROR ('Unable to locate DefaultData or SQLDataRoot registry key', 16, 1) WITH SETERROR

  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE', @key=@RegKey, @value_name='DefaultLog', @value=@LogDir OUTPUT
  IF @LogDir IS NULL AND @DataRoot IS NOT NULL SET @LogDir = @DataRoot + '\\Data'
  IF @LogDir IS NULL RAISERROR ('Unable to locate DefaultLog or SQLDataRoot registry key', 16, 1) WITH SETERROR

  DECLARE @sql VARCHAR(4000)
  SET @sql = 'RESTORE DATABASE #{quote_table_name(configuration.catalog_name)} FROM DISK = N''' + @BackupName + ''' WITH  FILE = 1,
  MOVE N''' + @DataLogicalName + ''' TO N''' + @DataDir + '\\#{configuration.catalog_name}.MDF'',
  MOVE N''' + @LogLogicalName + ''' TO N''' + @LogDir + '\\#{configuration.catalog_name}.LDF'',
  NOUNLOAD,
  REPLACE,
  STATS = 10
  '

  EXEC(@sql)
SQL
      end

      def get_backup_name_sql(configuration, backup_name)
        sql = 'DECLARE @BackupName VARCHAR(500)'
        if configuration.backup_location
          sql << "SET @BackupName = '#{configuration.backup_location}\\#{backup_name}.bak'"
        else
          sql << <<SQL
  SET @RegKey = 'SOFTWARE\\Microsoft\\Microsoft SQL Server\\' + @InstanceKey + '\\MSSQLServer'
  DECLARE @BackupDir VARCHAR(400)
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE', @key=@RegKey, @value_name='BackupDirectory', @value=@BackupDir OUTPUT
  IF @BackupDir IS NULL RAISERROR ('Unable to locate BackupDirectory registry key', 16, 1) WITH SETERROR
  SET @BackupName = @BackupDir + '\\#{backup_name}.bak'
SQL

        end
        sql
      end

      def get_instance_key_sql
        <<-SQL
  DECLARE @InstanceKey VARCHAR(400)
  EXEC xp_regread @rootkey='HKEY_LOCAL_MACHINE', @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\Instance Names\\SQL', @value_name=@@servicename, @value=@InstanceKey OUTPUT
        SQL
      end

      def setup_migrations
        if query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'tblMigration'").empty?
          execute("CREATE TABLE #{quote_table_name('dbo')}.#{quote_table_name('tblMigration')}(#{quote_column_name('Namespace')} varchar(50),#{quote_column_name('Migration')} varchar(255),#{quote_column_name('AppliedAt')} datetime)")
        end
      end

      def should_migrate?(namespace, migration_name)
        setup_migrations
        query("SELECT * FROM #{quote_table_name('dbo')}.#{quote_table_name('tblMigration')} WHERE #{quote_column_name('Namespace')} = #{quote_value(namespace)} AND #{quote_column_name('Migration')} = #{quote_value(migration_name)}").empty?
      end

      def mark_migration_as_run(namespace, migration_name)
        execute("INSERT INTO #{quote_table_name('dbo')}.#{quote_table_name('tblMigration')}(#{quote_column_name('Namespace')},#{quote_column_name('Migration')},#{quote_column_name('AppliedAt')}) VALUES (#{quote_value(namespace)}, #{quote_value(migration_name)}, GETDATE())")
      end

      def pre_fixture_import(table)
        identity_insert_sql = get_identity_insert_sql(table, true)
        execute(identity_insert_sql) if identity_insert_sql
      end

      def post_fixture_import(table)
        identity_insert_sql = get_identity_insert_sql(table, false)
        execute(identity_insert_sql) if identity_insert_sql
      end

      def pre_table_import(imp, table)
        pre_fixture_import(table)
      end

      def post_table_import(imp, table)
        post_fixture_import(table)
        if Dbt.configuration_for_key(imp.database.key).reindex_on_import?
          Dbt.runtime.info("Reindexing #{clean_table_name(table)}")
          execute("DBCC DBREINDEX (N'#{table}', '', 0) WITH NO_INFOMSGS")
        end
      end

      def post_data_module_import(imp, module_name)
        sql_prefix = 'DECLARE @DbName VARCHAR(100); SET @DbName = DB_NAME();'
        if Dbt.configuration_for_key(imp.database.key).shrink_on_import?
          # We are shrinking the database in case any of the import scripts created tables/columns and dropped them
          # later. This would leave large chunks of empty space in the underlying files. However it has to be done before
          # we reindex otherwise the indexes will be highly fragmented.
          Dbt.runtime.info('Shrinking database')
          execute("#{sql_prefix} DBCC SHRINKDATABASE(@DbName, 10, NOTRUNCATE) WITH NO_INFOMSGS")
          execute("#{sql_prefix} DBCC SHRINKDATABASE(@DbName, 10, TRUNCATEONLY) WITH NO_INFOMSGS")
        end

        if Dbt.configuration_for_key(imp.database.key).reindex_on_import?
          imp.database.repository.table_ordering(module_name).each do |table|
            Dbt.runtime.info("Reindexing #{clean_table_name(table)}")
            execute("DBCC DBREINDEX (N'#{table}', '', 0) WITH NO_INFOMSGS")
          end
        end
      end

      def post_database_import(imp)
        if Dbt.configuration_for_key(imp.database.key).reindex_on_import?
          sql_prefix = 'DECLARE @DbName VARCHAR(100); SET @DbName = DB_NAME();'

          Dbt.runtime.info('Updating statistics')
          execute('EXEC dbo.sp_updatestats')

          # This updates the usage details for the database. i.e. how much space is take for each index/table
          Dbt.runtime.info('Updating usage statistics')
          execute("#{sql_prefix} DBCC UPDATEUSAGE(@DbName) WITH NO_INFOMSGS, COUNT_ROWS")
        end
      end

      def column_names_for_table(table)
        sql = <<SQL
SELECT C.name as column_name
FROM sys.syscolumns C
WHERE C.id = OBJECT_ID('#{table}')
ORDER BY C.colid
SQL
        query(sql).map { |r| quote_column_name(r.values[0]) }
      end

      protected

      def has_identity_column(table)
        sql = <<-SQL
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE COLUMNPROPERTY(OBJECT_ID('#{table}'), COLUMN_NAME, 'IsIdentity') = 1
        SQL
        select_value(sql).to_s != '0'
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
        query(sql).map { |v| v.values[0] }
      end

      def current_database
        select_value('SELECT DB_NAME()')
      end

      def quote_column_name(name)
        "[#{name}]"
      end

      def quote_table_name(name)
        "[#{name}]"
      end

      def clean_table_name(table_name)
        table_name.tr('[]"' '', '')
      end

      def quote_value(value)
        case value
          when NilClass then
            'NULL'
          when String then
            "'#{value.to_s.gsub(/\'/, "''")}'"
          when TrueClass then
            '1'
          when FalseClass then
            '0'
          else
            value
        end
      end

      def select_database(database_name)
        if database_name.nil?
          execute('USE [msdb]')
        else
          execute("USE [#{database_name}]")
        end
      end

      def get_backup_file_list_locations
        <<SQL
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

   INSERT INTO @FileList EXEC('RESTORE FILELISTONLY FROM DISK = ''' + @BackupName + '''')
   SELECT TOP 1 @DataLogicalName = LogicalName FROM @FileList WHERE Type = 'D' ORDER BY DifferentialBaseLSN
   SELECT TOP 1 @LogLogicalName = LogicalName FROM @FileList WHERE Type = 'L' ORDER BY DifferentialBaseLSN
SQL
      end
    end
  end
end
