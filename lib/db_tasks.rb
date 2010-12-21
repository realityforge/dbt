# Note the following terminology is used throughout the plugin
# * database_key: a symbolic name of database. i.e. "central", "master", "core",
#   "ifis", "msdb" etc
# * env: a development environment. i.e. "test", "development", "production"
# * module_name: the name of the database directory in which sets of related database
#   files are stored. i.e. "Audit", "Auth", "Interpretation", ...
# * config_key: the name of entry in YAML file to look up configuration. Typically
#   constructed by database_key and env separated by an underscore. i.e.
#   "central_development", "master_test" etc.

# It should also be noted that the in some cases there is a database_key and
# module_key with the same name. This was due to legacy reasons and should be avoided
# in the future as it is confusing

class DbTasks

  class Config

    class << self
      attr_writer :environment

      def environment
        return 'development' unless @environment
        @environment
      end

      attr_writer :default_collation

      def default_collation
        return 'SQL_Latin1_General_CP1_CS_AS' unless @default_collation
        @default_collation
      end

      attr_writer :default_database

      def default_database
        return 'default' unless @default_database
        @default_database
      end

      # config_file is where the yaml config file is located
      attr_writer :config_filename

      def config_filename
        raise "config_filename not specified" unless @config_filename
        @config_filename
      end

      # log_filename is where the log file is created
      attr_writer :log_filename

      def log_filename
        raise "log_filename not specified" unless @log_filename
        @log_filename
      end

      # search_dirs is an array of paths that are searched in order for artifacts for each module
      attr_writer :default_search_dirs

      def default_search_dirs
        raise "default_search_dirs not specified" unless @default_search_dirs
        @default_search_dirs
      end

      attr_writer :default_up_dirs

      def default_up_dirs
        return ['.', 'types', 'views', 'functions', 'stored-procedures', 'misc'] unless @default_up_dirs
        @default_up_dirs
      end

      attr_writer :default_finalize_dirs

      def default_finalize_dirs
        return ['triggers', 'finalize'] unless @default_finalize_dirs
        @default_finalize_dirs
      end

      attr_writer :default_pre_import_dirs

      def default_pre_import_dirs
        return ['import-hooks/pre'] unless @default_post_import_dirs
        @default_post_import_dirs
      end

      attr_writer :default_post_import_dirs

      def default_post_import_dirs
        return ['import-hooks/post'] unless @default_post_import_dirs
        @default_post_import_dirs
      end

      attr_writer :default_down_dirs

      def default_down_dirs
        return ['down'] unless @default_down_dirs
        @default_down_dirs
      end
    end
  end

  module FilterContainer
    def add_filter(&block)
      self.filters << block
    end

    def add_database_name_filter(pattern, database_key)
      add_filter do |current_config, env, sql|
        DbTasks.filter_database_name(sql, pattern, current_config, DbTasks.config_key(database_key, env), false)
      end
    end

    # Filter the SQL files replacing specified pattern with specified value
    def add_property_filter(pattern, value)
      #noinspection RubyUnusedLocalVariable
      add_filter do |current_config, env, sql|
        sql.gsub(pattern, value)
      end
    end

    # Makes the import scripts support statements such as
    #   ASSERT_ROW_COUNT(1)
    #   ASSERT_ROW_COUNT(SELECT COUNT(*) FROM Foo)
    #   ASSERT_UNCHANGED_ROW_COUNT()
    #   ASSERT(@Id IS NULL)
    #
    def add_import_assert_filters
      #noinspection RubyUnusedLocalVariable
      add_filter do |current_config, env, sql|
        sql = sql.gsub(/ASSERT_UNCHANGED_ROW_COUNT\(\)/, <<SQL)
IF (SELECT COUNT(*) FROM @@TARGET@@.@@TABLE@@) != (SELECT COUNT(*) FROM @@SOURCE@@.\\1)
BEGIN
  RAISERROR ('Actual row count for @@TABLE@@ does not match expected rowcount', 16, 1) WITH SETERROR
END
SQL
        sql = sql.gsub(/ASSERT_ROW_COUNT\((.*)\)/, <<SQL)
IF (SELECT COUNT(*) FROM @@TARGET@@.@@TABLE@@) != (\\1)
BEGIN
  RAISERROR ('Actual row count for @@TABLE@@ does not match expected rowcount', 16, 1) WITH SETERROR
END
SQL
        sql = sql.gsub(/ASSERT\((.+)\)/, <<SQL)
IF NOT (\\1)
BEGIN
  RAISERROR ('Failed to assert \\1', 16, 1) WITH SETERROR
END
SQL
        sql
      end
    end

    def filters
      @filters ||= []
    end
  end

  class ImportDefinition
    include FilterContainer

    def initialize(database, key, options)
      @database = database
      @key = key
      @modules = options[:modules] if options[:modules]
      @dir = options[:dir] if options[:dir]
      @reindex = options[:reindex] if options[:reindex]
      @pre_import_dirs = options[:pre_import_dirs] if options[:pre_import_dirs]
      @post_import_dirs = options[:post_import_dirs] if options[:post_import_dirs]
    end

    attr_accessor :database
    attr_accessor :key

    def modules
      @modules || database.modules
    end

    def dir
      @dir || "import"
    end

    attr_writer :reindex

    def reindex?
      @reindex || false
    end

    attr_writer :pre_import_dirs

    def pre_import_dirs
      @pre_import_dirs || DbTasks::Config.default_pre_import_dirs
    end

    attr_writer :post_import_dirs

    def post_import_dirs
      @post_import_dirs || DbTasks::Config.default_post_import_dirs
    end

    def filters
      data.base.filters + @filters
    end
  end

  class DatabaseDefinition
    include FilterContainer

    def initialize(key, modules, options)
      @key = key
      @modules = modules
      @collation = DbTasks::Config.default_collation
      @backup = options[:backup] if options[:backup]
      @restore = options[:restore] if options[:restore]
      @schema_groups = options[:schema_groups] if options[:schema_groups]
      @datasets = options[:datasets] if options[:datasets]
      @collation = options[:collation] if options[:collation]
      @schema_overrides = options[:schema_overrides] if options[:schema_overrides]

      @imports = {}
      imports_config = options[:imports]
      if imports_config
        imports_config.keys.each do |import_key|
          import_config = imports_config[import_key]
          if import_config
            @imports[import_key] = ImportDefinition.new(self, import_key, import_config)
          end
        end
      end
    end

    # symbolic name of database
    attr_reader :key

    # List of modules to process for database
    attr_reader :imports

    # List of import configurations
    attr_reader :modules

    # Database version. Stuffed as an extended property and used when creating filename.
    attr_accessor :version

    # The collation name for database. Nil means take the dbt default_collation, if that is nil then take db default
    attr_accessor :collation

    attr_writer :search_dirs

    def search_dirs
      @search_dirs || DbTasks::Config.default_search_dirs
    end

    attr_writer :up_dirs

    # Return the list of dirs to process when "upping" module
    def up_dirs
      @up_dirs || DbTasks::Config.default_up_dirs
    end

    attr_writer :down_dirs

    # Return the list of dirs to process when "downing" module
    def down_dirs
      @down_dirs || DbTasks::Config.default_down_dirs
    end

    attr_writer :finalize_dirs

    # Return the list of dirs to process when finalizing module.
    # i.e. Getting database ready for use. Often this is the place to add expensive triggers, constraints and indexes
    # after the import
    def finalize_dirs
      @finalize_dirs || DbTasks::Config.default_finalize_dirs
    end

    attr_writer :datasets

    # List of datasets that should be defined.
    def datasets
      @datasets || []
    end

    attr_writer :create_by_import

    # Should a task be created to create by import?
    def create_by_import?
      @create_by_import || false
    end

    attr_writer :backup

    # Should the a backup task be defined for database?
    def backup?
      @backup || false
    end

    attr_writer :restore

    # Should the a restore task be defined for database?
    def restore?
      @restore || false
    end

    # Map of module => schema overrides
    # i.e. What database schema is created for a specific module
    def schema_overrides
      @schema_overrides || {}
    end

    def schema_name_for_module(module_name)
      schema_overrides[module_name] || module_name
    end

    # A map of names => schema groups.
    # A schema group is a set of database schemas that can be managed as a group. i.e. Upped or downed together.
    def schema_groups
      @schema_groups || {}
    end

    def define_table_order_resolver(&block)
      @table_order_resolver = block
    end

    def table_ordering(module_name)
      raise "No table resolver so unable to determine table ordering for module #{module_name}" unless @table_order_resolver
      @table_order_resolver.call(module_name)
    end

    # Enable domgen support. Assume the database is associated with a single repository
    # definition, a single task to generate sql etc.
    def enable_domgen(repository_key, load_task_name, generate_task_name)
      define_table_order_resolver do |schema_key|
        require 'domgen'
        schema = Domgen.repository_by_name(repository_key).data_module_by_name(schema_key.to_s)
        schema.object_types.select { |object_type| !object_type.abstract? }.collect do |object_type|
          object_type.sql.qualified_table_name
        end
      end
      task "dbt:#{key}:load_config" => load_task_name
      task "dbt:#{key}:pre_build" => generate_task_name
    end
  end

  @@defined_init_tasks = false
  @@database_driver_hooks = []
  @@databases = {}

  def self.init(database_key, env)
    setup_connection(config_key(database_key, env))
  end

  def self.init_msdb
    setup_connection("msdb")
  end

  def self.add_database_driver_hook(&block)
    @@database_driver_hooks << block
  end

  def self.add_database(database_key, modules, options = {})
    self.define_basic_tasks

    raise "Database with key #{database_key} already defined." if @@databases.has_key?(database_key)

    database = DatabaseDefinition.new(database_key, modules, options)
    @@databases[database_key] = database

    yield database if block_given?

    define_tasks_for_database(database)
  end

  def self.run_sql_in_dir(database, env, label, dir)
    check_dir(label, dir)
    Dir["#{dir}/*.sql"].sort.each do |sp|
      info("#{label}: #{File.basename(sp)}")
      run_filtered_sql(database, env, IO.readlines(sp).join)
    end
  end

  def self.filter_database_name(sql, pattern, current_config_key, target_database_config_key, optional = true)
    return sql if optional && ActiveRecord::Base.configurations[target_database_config_key].nil?
    sql.gsub(pattern, get_db_spec(current_config_key, target_database_config_key))
  end

  def self.dump_tables_to_fixtures(tables, fixture_dir)
    tables.each do |table_name|
      i = 0
      File.open("#{fixture_dir}/#{table_name}.yml", 'wb') do |file|
        print("Dumping #{table_name}\n")
        const_name = :"DUMP_SQL_FOR_#{table_name.gsub('.', '_')}"
        if Object.const_defined?(const_name)
          sql = Object.const_get(const_name)
        else
          sql = "SELECT * FROM #{table_name}"
        end

        dump_class = Class.new(ActiveRecord::Base) do
          set_table_name table_name
        end

        records = YAML::Omap.new
        dump_class.find_by_sql(sql).collect do |record|
          records["r#{i += 1}"] = record.attributes
        end

        file.write records.to_yaml
      end
    end
  end

  def self.load_modules_fixtures(database_key, env, module_name)
    database = database_for_key(database_key)
    init(database.key, env)
    load_fixtures(database, module_name)
  end

  private

  def self.database_for_key(database_key)
    database = @@databases[database_key]
    raise "Missing database for key #{database_key}" unless database
    database
  end

  def self.define_tasks_for_database(database)
    task "dbt:#{database.key}:load_config" => ["dbt:load_config"]

    # Database dropping

    desc "Drop the #{database.key} database."
    task "dbt:#{database.key}:drop" => ["dbt:#{database.key}:load_config"] do
      info("**** Dropping database: #{database.key} ****")
      drop(database, DbTasks::Config.environment)
    end

    # Database creation

    task "dbt:#{database.key}:banner" do
      info("**** Creating database: #{database.key} (Environment: #{DbTasks::Config.environment}) ****")
    end

    task "dbt:#{database.key}:pre_build" => ["dbt:#{database.key}:load_config", 'dbt:pre_build']

    desc "Create the #{database.key} database."
    task "dbt:#{database.key}:create" => ["dbt:#{database.key}:banner", "dbt:#{database.key}:pre_build"] do
      perform_create_action(database, DbTasks::Config.environment, :up)
      perform_create_action(database, DbTasks::Config.environment, :finalize)
    end

    # Data set loading etc
    database.datasets.each do |dataset_name|
      desc "Loads #{dataset_name} data"
      task "dbt:#{database.key}:datasets:#{dataset_name}" => ["dbt:#{database.key}:load_config"] do
        database.modules.each do |module_name|
          load_dataset(database, DbTasks::Config.environment, module_name, dataset_name)
        end
      end
    end

    # Import tasks
    database.imports.values.each do |imp|
      define_import_task("dbt:#{database.key}", imp, "contents")
    end

    database.schema_groups.each_pair do |schema_group_name, schemas|
      define_schema_group_tasks(database, schema_group_name, schemas)
    end

    if database.create_by_import?
      database.imports.values.each do |imp|
        desc "Create the #{database.key} database by import."
        task "dbt:#{database.key}:create_by_import" => ["dbt:#{database.key}:banner", "dbt:#{database.key}:pre_build"] do
          perform_create_action(database, DbTasks::Config.environment, :up)
          perform_import_action(imp, DbTasks::Config.environment, false)
          perform_create_action(database, DbTasks::Config.environment, :finalize)
        end
      end
    end

    if database.backup?
      desc "Perform backup of #{database.key} database"
      task "dbt:#{database.key}:backup" => ["dbt:#{database.key}:load_config"] do
        backup(database, DbTasks::Config.environment)
      end
    end

    if database.restore?
      desc "Perform restore of #{database.key} database"
      task "dbt:#{database.key}:restore" => ["dbt:#{database.key}:load_config"] do
        DbTasks.restore(database, DbTasks::Config.environment)
      end
    end
  end

  def self.define_schema_group_tasks(database, schema_group_name, schemas)
    desc "Up the #{schema_group_name} schema group in the #{database.key} database."
    task "dbt:#{database.key}:#{schema_group_name}:up" => ["dbt:#{database.key}:load_config", "dbt:#{database.key}:pre_build"] do
      info("**** Upping schema group: #{schema_group_name} (Database: #{database.key}, Environment: #{DbTasks::Config.environment}) ****")
      database.modules.each do |module_name|
        schema_name = database.schema_name_for_module(module_name)
        next unless schemas.include?(schema_name)
        create_module(database, DbTasks::Config.environment, module_name, schema_name, :up)
        create_module(database, DbTasks::Config.environment, module_name, schema_name, :finalize)
      end
    end

    desc "Down the #{schema_group_name} schema group in the #{database.key} database."
    task "dbt:#{database.key}:#{schema_group_name}:down" => ["dbt:#{database.key}:load_config", "dbt:#{database.key}:pre_build"] do
      info("**** Downing schema group: #{schema_group_name} (Database: #{database.key}, Environment: #{DbTasks::Config.environment}) ****")
      init(database.key, DbTasks::Config.environment)
      database.modules.reverse.each do |module_name|
        schema_name = database.schema_name_for_module(module_name)
        next unless schemas.include?(schema_name)
        process_module(database.key, DbTasks::Config.environment, module_name, :down)
      end
      schema_2_module = {}
      database.modules.each do |module_name|
        schema_name = database.schema_name_for_module(module_name)
        (schema_2_module[schema_name] ||= []) << module_name
      end
      schemas.reverse.each do |schema_name|
        drop_schema(database, schema_name, schema_2_module[schema_name])
      end
    end

    database.imports.values.each do |imp|
      import_modules = imp.modules.select do |module_name|
        schemas.include?(database.schema_name_for_module(module_name))
      end
      if !import_modules.empty?
        description = "contents of the #{schema_group_name} schema group"
        define_import_task("dbt:#{database.key}:#{schema_group_name}", imp, description)
      end
    end
  end

  def self.import(database, env, module_name, import_dir, reindex, should_perform_delete)
    ordered_tables = database.table_ordering(module_name)

    # check the database configurations are set
    target_config = config_key(database.key, env)
    source_config = config_key(database.key, "import")
    get_config(target_config)
    get_config(source_config)

    trace("Database Import [#{physical_database_name(database.key, env)}]: module_name=#{module_name}, database_key=#{database.key}, env=#{env}, source_key=#{source_config} target_key=#{target_config}")
    setup_connection(target_config)

    # Iterate over module in dependency order doing import as appropriate
    # Note: that tables with initial fixtures are skipped
    tables = ordered_tables.reject do |table|
      fixture_for_creation(database, module_name, table)
    end
    if should_perform_delete
      tables.reverse.each do |table|
        info("Deleting #{table}")
        run_import_sql(database, env, table, "DELETE FROM @@TARGET@@.@@TABLE@@")
      end
    end

    tables.each do |table|
      perform_import(database, env, module_name, table, import_dir)
    end

    if reindex
      tables.each do |table|
        info("Reindexing #{table}")
        run_import_sql(database, env, table, "DBCC DBREINDEX (N'@@TARGET@@.@@TABLE@@', '', 0) WITH NO_INFOMSGS")
      end

      # We are shrinking the database in case any of the import scripts created tables/columns and dropped them
      # later. This would leave large chunks of empty space in the underlying files. However it has to be done before
      # we reindex otherwise the indexes will be highly fragmented.
      info("Shrinking database")
      run_import_sql(database, env, nil, "DBCC SHRINKDATABASE(N'@@TARGET@@', 10, NOTRUNCATE) WITH NO_INFOMSGS")
      run_import_sql(database, env, nil, "DBCC SHRINKDATABASE(N'@@TARGET@@', 10, TRUNCATEONLY) WITH NO_INFOMSGS")
      info("Updating statistics")
      run_import_sql(database, env, nil, "EXEC @@TARGET@@.dbo.sp_updatestats")

      # This updates the usage details for the database. i.e. how much space is take for each index/table 
      info("Updating usage statistics")
      run_import_sql(database, env, nil, "DBCC UPDATEUSAGE(N'@@TARGET@@') WITH NO_INFOMSGS, COUNT_ROWS")
    end
  end

  def self.backup(database, env)
    phsyical_name = physical_database_name(database.key, env)
    info("Backup Database [#{phsyical_name}]: database_key=#{database.key}, env=#{env}")
    registry_key = instance_registry_key(database.key, env)
    sql = <<SQL
USE [msdb]

  DECLARE @BackupDir VARCHAR(400)
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{registry_key}\\MSSQLServer',
    @value_name='BackupDirectory',
    @value=@BackupDir OUTPUT
  IF @BackupDir IS NULL RAISERROR ('Unable to locate BackupDirectory registry key', 16, 1) WITH SETERROR
  DECLARE @BackupName VARCHAR(500)
  SET @BackupName = @BackupDir + '\\#{phsyical_name}.bak'

BACKUP DATABASE [#{phsyical_name}] TO DISK = @BackupName
WITH FORMAT, INIT, NAME = N'POST_CI_BACKUP', SKIP, NOREWIND, NOUNLOAD, STATS = 10
SQL
    init(database.key, env)
    run_sql_statement(sql)
  end

  def self.restore(database, env)
    phsyical_name = physical_database_name(database.key, env)
    info("Restore Database [#{phsyical_name}]: database_key=#{database.key}, env=#{env}")
    registry_key = instance_registry_key(database.key, env)
    sql = <<SQL
  USE [msdb]
  DECLARE @TargetDatabase VARCHAR(400)
  DECLARE @SourceDatabase VARCHAR(400)
  SET @TargetDatabase = '#{phsyical_name}'
  SET @SourceDatabase = '#{restore_from(database.key, env)}'

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
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{registry_key}\\MSSQLServer',
    @value_name='DefaultData',
    @value=@DataDir OUTPUT
  IF @DataDir IS NULL RAISERROR ('Unable to locate DefaultData registry key', 16, 1) WITH SETERROR
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{registry_key}\\MSSQLServer',
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
    init(database.key, env)
    run_sql_statement("ALTER DATABASE [#{phsyical_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
    run_sql_statement(sql)
  end

  def self.drop(database, env)
    init_msdb
    phsyical_name = physical_database_name(database.key, env)

    sql = if force_drop?(database.key, env)
      <<SQL
USE [msdb]
GO
  IF EXISTS
    ( SELECT *
      FROM  sys.master_files
      WHERE state = 0 AND db_name(database_id) = '#{phsyical_name}')
    ALTER DATABASE [#{phsyical_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
SQL
    else
      ''
    end

    sql << <<SQL
USE [msdb]
GO
  IF EXISTS
    ( SELECT *
      FROM  sys.master_files
      WHERE state = 0 AND db_name(database_id) = '#{phsyical_name}')
    DROP DATABASE [#{phsyical_name}]
GO
SQL
    trace("Database Drop [#{phsyical_name}]: database_key=#{database.key}, env=#{env}")
    run_filtered_sql(database, env, sql)
  end

  def self.create_module(database, env, module_name, schema_name, mode)
    init(database.key, env)
    trace("Module #{mode == :up ? "create" : "finalize"} [#{physical_database_name(database.key, env)}]: module=#{module_name}, database_key=#{database.key}, env=#{env}")
    if ActiveRecord::Base.connection.select_all("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
      run_filtered_sql(database, env, "CREATE SCHEMA [#{schema_name}]")
    end
    process_module(database, env, module_name, mode)
  end

  def self.define_import_task(prefix, imp, description)
    is_default_import = imp.key == :default
    desc_prefix = is_default_import ? 'Import' : "#{imp.key.to_s.capitalize} import"

    taskname = is_default_import ? :import : :"#{imp.key}-import"
    desc "#{desc_prefix} #{description} of the #{imp.database.key} database."
    task "#{prefix}:#{taskname}" => ["dbt:#{imp.database.key}:load_config"] do
      perform_import_action(imp, DbTasks::Config.environment, true)
    end
  end

  def self.perform_create_action(database, env, mode)
    database.modules.each_with_index do |module_name, idx|
      create_database(database, env) if idx == 0
      schema_name = database.schema_name_for_module(module_name)
      create_module(database, env, module_name, schema_name, mode)
    end
  end

  def self.perform_import_action(imp, env, should_perform_delete)
    init(imp.database.key, env)
    imp.pre_import_dirs.each do |dir|
      run_sql_in_dirs(imp.database, env, "pre-import", dirs_for_database(imp.database, dir), true)
    end
    imp.modules.each do |module_name|
      import(imp.database, env, module_name, imp.dir, imp.reindex?, should_perform_delete)
    end
    imp.post_import_dirs.each do |dir|
      run_sql_in_dirs(imp.database, env, "post-import", dirs_for_database(imp.database, dir), true)
    end
  end

  def self.define_basic_tasks
    if !@@defined_init_tasks
      task "dbt:load_config" do
        require 'activerecord'
        require 'active_record/fixtures'
        @@database_driver_hooks.each do |database_hook|
          database_hook.call
        end
        ActiveRecord::Base.configurations = YAML::load(ERB.new(IO.read(DbTasks::Config.config_filename)).result)
      end

      task "dbt:pre_build" => ["dbt:load_config"]

      @@defined_init_tasks = true
    end
  end

  def self.config_key(database_key, env)
    database_key.to_s == DbTasks::Config.default_database.to_s ? env : "#{database_key}_#{env}"
  end

  def self.run_import_sql(database, env, table, sql, change_to_msdb = true)
    sql = filter_sql("msdb", "import", sql, database.filters)
    sql = sql.gsub(/@@TABLE@@/, table) if table
    sql = filter_database_name(sql, /@@SOURCE@@/, "msdb", config_key(database.key, "import"))
    sql = filter_database_name(sql, /@@TARGET@@/, "msdb", config_key(database.key, env))
    c = ActiveRecord::Base.connection
    current_database = physical_database_name(database.key, env)
    if change_to_msdb
      c.execute "USE [msdb]"
      run_sql(sql)
      c.execute "USE [#{current_database}]"
    else
      run_sql(sql)
    end
  end

  def self.generate_standard_import_sql(table)
    sql = "INSERT INTO @@TARGET@@.#{table}("
    columns = ActiveRecord::Base.connection.columns(table).collect { |c| "[#{c.name}]" }
    sql += columns.join(', ')
    sql += ")\n  SELECT "
    sql += columns.collect { |c| c == '[BatchID]' ? "0" : c }.join(', ')
    sql += " FROM @@SOURCE@@.#{table}\n"
    sql
  end

  def self.perform_standard_import(database, env, table)
    run_import_sql(database, env, table, generate_standard_import_sql(table))
  end

  def self.perform_import(database, env, module_name, table, import_dir)
    has_identity = has_identity_column(table)

    run_import_sql(database, env, table, "SET IDENTITY_INSERT @@TARGET@@.@@TABLE@@ ON") if has_identity
    run_import_sql(database, env, table, "EXEC sp_executesql \"DISABLE TRIGGER ALL ON @@TARGET@@.@@TABLE@@\"", false)

    fixture_file = fixture_for_import(database, module_name, table, import_dir)
    sql_file = sql_for_import(database, module_name, table, import_dir)
    is_sql = !fixture_file && sql_file

    info("Importing #{table} (By #{fixture_file ? 'F' : is_sql ? 'S' : "D"})")
    if fixture_file
      Fixtures.create_fixtures(File.dirname(fixture_file), table)
    elsif is_sql
      run_import_sql(database, env, table, IO.readlines(sql_file).join)
    else
      perform_standard_import(database, env, table)
    end

    run_import_sql(database, env, table, "EXEC sp_executesql \"ENABLE TRIGGER ALL ON @@TARGET@@.@@TABLE@@\"", false)
    run_import_sql(database, env, table, "SET IDENTITY_INSERT @@TARGET@@.@@TABLE@@ OFF") if has_identity
  end

  def self.has_identity_column(table)
    ActiveRecord::Base.connection.columns(table).each do |c|
      return true if c.identity == true
    end
    false
  end

  def self.setup_connection(config_key)
    ActiveRecord::Base.colorize_logging = false
    ActiveRecord::Base.establish_connection(get_config(config_key))
    FileUtils.mkdir_p File.dirname(DbTasks::Config.log_filename)
    ActiveRecord::Base.logger = Logger.new(File.open(DbTasks::Config.log_filename, 'a'))
    ActiveRecord::Migration.verbose = ENV["VERBOSE"] ? ENV["VERBOSE"] == "true" : false
  end

  def self.create_database(database, env)
    return if no_create?(database.key, env)
    init_msdb
    drop(database, env)
    physical_name = physical_database_name(database.key, env)
    if database.version.nil?
      db_filename = physical_name
    else
      db_filename = "#{physical_name}_#{database.version.gsub(/\./, '_')}"
    end
    base_data_path = data_path(database.key, env)
    base_log_path = log_path(database.key, env)

    db_def = base_data_path ? "ON PRIMARY (NAME = [#{db_filename}], FILENAME='#{base_data_path}#{"\\"}#{db_filename}.mdf')" : ""
    log_def = base_log_path ? "LOG ON (NAME = [#{db_filename}_LOG], FILENAME='#{base_log_path}#{"\\"}#{db_filename}.ldf')" : ""

    collation_def = database.collation ? "COLLATE #{database.collation}" : ""

    sql = <<SQL
CREATE DATABASE [#{physical_name}] #{db_def} #{log_def} #{collation_def}
GO
ALTER DATABASE [#{physical_name}] SET CURSOR_DEFAULT LOCAL
ALTER DATABASE [#{physical_name}] SET CURSOR_CLOSE_ON_COMMIT ON

ALTER DATABASE [#{physical_name}] SET AUTO_CREATE_STATISTICS ON
ALTER DATABASE [#{physical_name}] SET AUTO_UPDATE_STATISTICS ON
ALTER DATABASE [#{physical_name}] SET AUTO_UPDATE_STATISTICS_ASYNC ON

ALTER DATABASE [#{physical_name}] SET ANSI_NULL_DEFAULT ON
ALTER DATABASE [#{physical_name}] SET ANSI_NULLS ON
ALTER DATABASE [#{physical_name}] SET ANSI_PADDING ON
ALTER DATABASE [#{physical_name}] SET ANSI_WARNINGS ON
ALTER DATABASE [#{physical_name}] SET ARITHABORT ON
ALTER DATABASE [#{physical_name}] SET CONCAT_NULL_YIELDS_NULL ON
ALTER DATABASE [#{physical_name}] SET QUOTED_IDENTIFIER ON
-- NUMERIC_ROUNDABORT OFF is required for filtered indexes. The optimizer will also
-- not consider indexed views if the setting is not set. 
ALTER DATABASE [#{physical_name}] SET NUMERIC_ROUNDABORT OFF
ALTER DATABASE [#{physical_name}] SET RECURSIVE_TRIGGERS ON

ALTER DATABASE [#{physical_name}] SET RECOVERY SIMPLE

GO
  USE [#{physical_name}]
SQL
    trace("Database Create [#{physical_name}]: database_key=#{database.key}, env=#{env}")
    run_filtered_sql(database, env, sql)
    if !database.version.nil?
      sql = <<SQL
    EXEC sys.sp_addextendedproperty @name = N'DatabaseSchemaVersion', @value = N'#{database.version}'
SQL
      run_filtered_sql(database, env, sql)
    end
  end

  def self.process_module(database, env, module_name, mode)
    dirs = mode == :up ? database.up_dirs : mode == :down ? database.down_dirs : database.finalize_dirs
    dirs.each do |dir|
      run_sql_in_dirs(database, env, (dir == '.' ? 'Base' : dir.humanize), dirs_for_module(database, module_name, dir))
    end
    load_fixtures(database, module_name) if mode == :up
  end

  def self.load_fixtures(database, module_name)
    load_fixtures_from_dirs(database, module_name, dirs_for_module(database, module_name, 'fixtures'))
  end

  def self.check_dir(name, dir)
    raise "#{name} in missing dir #{dir}" unless File.exists?(dir)
  end

  def self.load_dataset(database, env, module_name, dataset_name)
    init(database.key, env)
    load_fixtures_from_dirs(database, module_name, dirs_for_module(database, module_name, "datasets/#{dataset_name}"))
  end

  def self.load_fixtures_from_dirs(database, module_name, dirs)
    require 'active_record/fixtures'
    dir = dirs.select { |dir| File.exists?(dir) }[0]
    return unless dir
    files = []
    database.table_ordering(module_name).each do |t|
      files += [t] if File.exist?("#{dir}/#{t}.yml")
    end
    info("Loading fixtures: #{files.join(',')}")
    Fixtures.create_fixtures(dir, files)
  end

  def self.run_sql(sql)
    sql.gsub(/\r/, '').split(/(\s|^)GO(\s|$)/).each do |ddl|
      # Transaction required to work around a bug that sometimes leaves last
      # SQL command before shutting the connection un committed.
      ActiveRecord::Base.connection.transaction do
        run_sql_statement(ddl)
      end
    end
  end

  def self.run_sql_statement(sql)
    ActiveRecord::Base.connection.execute(sql, nil)
  end

  def self.drop_schema(database, schema_name, modules)
    database_objects("SQL_STORED_PROCEDURE", schema_name).each { |name| run_sql("DROP PROCEDURE #{name}") }
    database_objects("SQL_SCALAR_FUNCTION", schema_name).each { |name| run_sql("DROP FUNCTION #{name}") }
    database_objects("SQL_INLINE_TABLE_VALUED_FUNCTION", schema_name).each { |name| run_sql("DROP FUNCTION #{name}") }
    database_objects("SQL_TABLE_VALUED_FUNCTION", schema_name).each { |name| run_sql("DROP FUNCTION #{name}") }
    database_objects("VIEW", schema_name).each { |name| run_sql("DROP VIEW #{name}") }
    modules.reverse.each do |module_name|
      database.table_ordering(module_name).reverse.each do |t|
        run_sql("DROP TABLE #{t}")
      end
    end
    run_sql("DROP SCHEMA #{schema_name}")
  end

  def self.database_objects(object_type, schema_name)
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

  def self.get_config(config_key)
    require 'activerecord'
    c = ActiveRecord::Base.configurations[config_key]
    raise "Missing config for #{config_key}" unless c
    c
  end

  def self.get_db_spec(current_config_key, target_config_key)
    current = ActiveRecord::Base.configurations[current_config_key]
    target = get_config(target_config_key)
    if current.nil? || current['host'] != target['host']
      "#{target['host']}.#{target['database']}"
    else
      target['database']
    end
  end

  def self.run_filtered_sql(database, env, sql)
    sql = filter_sql(config_key(database.key, env), env, sql, database.filters)
    run_sql(sql)
  end

  def self.filter_sql(config_key, env, sql, filters)
    filters.each do |filter|
      sql = filter.call(config_key, env, sql)
    end
    sql = filter_database_name(sql, /@@SELF@@/, config_key, config_key)
    sql
  end

  def self.run_sql_in_dirs(database, env, label, dirs, is_import = false)
    dirs.each do |dir|
      run_sql_in_dir(database, env, label, dir, is_import) if File.exists?(dir)
    end
  end

  def self.run_sql_in_dir(database, env, label, dir, is_import)
    check_dir(label, dir)
    Dir["#{dir}/*.sql"].sort.each do |sp|
      info("#{label}: #{File.basename(sp)}")
      sql = IO.readlines(sp).join
      if is_import
        run_import_sql(database, env, nil, sql)
      else
        run_filtered_sql(database, env, sql)
      end
    end
  end

  def self.dirs_for_module(database, module_name, subdir = nil)
    database.search_dirs.map { |d| "#{d}/#{module_name}#{ subdir ? "/#{subdir}" : ''}" }
  end

  def self.dirs_for_database(database, subdir)
    database.search_dirs.map { |d| "#{d}/#{subdir}" }
  end

  def self.first_file_from(files)
    files.each do |file|
      if File.exist?(file)
        return file
      end
    end
    nil
  end

  def self.fixture_for_creation(database, module_name, table)
    first_file_from(dirs_for_module(database, module_name, "fixtures/#{table}.yml"))
  end

  def self.fixture_for_import(database, module_name, table, import_dir)
    first_file_from(dirs_for_module(database, module_name, "#{import_dir}/#{table}.yml"))
  end

  def self.sql_for_import(database, module_name, table, import_dir)
    first_file_from(dirs_for_module(database, module_name, "#{import_dir}/#{table}.sql"))
  end

  def self.info(message)
    puts message
  end

  def self.trace(message)
    puts message if ActiveRecord::Migration.verbose
  end

  def self.no_create?(database_key, env)
    true == config_value(database_key, env, "no_create", true)
  end

  def self.force_drop?(database_key, env)
    true == config_value(database_key, env, "force_drop", true)
  end

  def self.data_path(database_key, env)
    config_value(database_key, env, "data_path", true)
  end

  def self.log_path(database_key, env)
    config_value(database_key, env, "log_path", true)
  end

  def self.restore_from(database_key, env)
    config_value(database_key, env, "restore_from", false)
  end

  def self.instance_registry_key(database_key, env)
    config_value(database_key, env, "instance_registry_key", false)
  end

  def self.physical_database_name(database_key, env)
    config_value(database_key, env, "database", false)
  end

  def self.config_value(database_key, env, config_param_name, allow_nil)
    config_key = config_key(database_key, env)
    value = get_config(config_key)[config_param_name]
    raise "Unable to locate configuration value named #{config_param_name} in section #{config_key}" if !allow_nil && value.nil?
    value
  end
 
end
