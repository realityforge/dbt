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

require 'db_doc'

class DbTasks

  class Config

    class << self
      attr_writer :environment

      def environment
        @environment || 'development'
      end

      attr_writer :default_collation

      def default_collation
        @default_collation || 'SQL_Latin1_General_CP1_CS_AS'
      end

      attr_writer :default_database

      def default_database
        @default_database || :default
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
        @default_up_dirs || ['.', 'types', 'views', 'functions', 'stored-procedures', 'misc']
      end

      attr_writer :default_finalize_dirs

      def default_finalize_dirs
        @default_finalize_dirs || ['triggers', 'finalize']
      end

      attr_writer :default_pre_import_dirs

      def default_pre_import_dirs
        @default_pre_import_dirs || ['import-hooks/pre']
      end

      attr_writer :default_post_import_dirs

      def default_post_import_dirs
        @default_post_import_dirs || ['import-hooks/post']
      end

      attr_writer :default_down_dirs

      def default_down_dirs
        @default_down_dirs || ['down']
      end

      attr_writer :index_file_name

      def index_file_name
        @index_file_name || 'index.txt'
      end

    end
  end

  module FilterContainer
    def add_filter(&block)
      self.filters << block
    end

    def add_database_name_filter(pattern, database_key)
      add_filter do |sql|
        DbTasks.filter_database_name(sql, pattern, DbTasks.config_key(database_key), false)
      end
    end

    # Filter the SQL files replacing specified pattern with specified value
    def add_property_filter(pattern, value)
      add_filter do |sql|
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
      add_filter do |sql|
        sql = sql.gsub(/ASSERT_UNCHANGED_ROW_COUNT\(\)/, <<SQL)
IF (SELECT COUNT(*) FROM @@TARGET@@.@@TABLE@@) != (SELECT COUNT(*) FROM @@SOURCE@@.@@TABLE@@)
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
      @reindex.nil? ? true : @reindex
    end

    attr_writer :shrink

    def shrink?
      @shrink.nil? ? false : @shrink
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

  class ModuleGroupDefinition

    def initialize(database, key, options)
      @database = database
      @key = key
      @modules = options[:modules]
      @import_enabled = options[:import_enabled]
    end

    attr_accessor :database
    attr_accessor :key

    attr_accessor :modules

    def import_enabled?
      @import_enabled.nil? ? false : @import_enabled
    end

  end

  class DatabaseDefinition
    include FilterContainer

    def initialize(key, options)
      @key = key
      @collation = DbTasks::Config.default_collation
      @modules = options[:modules] if options[:modules]
      @backup = options[:backup] if options[:backup]
      @restore = options[:restore] if options[:restore]
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
      @module_groups = {}
      module_groups_config = options[:module_groups]
      if module_groups_config
        module_groups_config.keys.each do |module_group_key|
          module_group_config = module_groups_config[module_group_key]
          if module_group_config
            @module_groups[module_group_key] = ModuleGroupDefinition.new(self, module_group_key, module_group_config)
          end
        end
      end
    end

    # symbolic name of database
    attr_reader :key

    # List of modules to import
    attr_reader :imports

    # List of module_groups configs
    attr_reader :module_groups

    attr_writer :modules

    # List of modules to process for database
     def modules
       @modules = @modules.call if @modules.is_a?(Proc)
       @modules
     end

    # Database version. Stuffed as an extended property and used when creating filename.
    attr_accessor :version

    # The collation name for database. Nil means take the dbt default_collation, if that is nil then take db default
    attr_accessor :collation

    attr_writer :search_dirs

    def search_dirs
      @search_dirs || DbTasks::Config.default_search_dirs
    end

    def dirs_for_database(subdir)
      search_dirs.map { |d| "#{d}/#{subdir}" }
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

    attr_writer :enable_separate_import_task

    def enable_separate_import_task?
      @enable_separate_import_task.nil? ? false : @enable_separate_import_task
    end

    attr_writer :enable_import_task_as_part_of_create

    def enable_import_task_as_part_of_create?
      @enable_import_task_as_part_of_create.nil? ? false : @enable_import_task_as_part_of_create
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
      define_table_order_resolver do |module_key|
        require 'domgen'
        data_module = Domgen.repository_by_name(repository_key).data_module_by_name(module_key.to_s)
        data_module.object_types.select { |object_type| !object_type.abstract? }.collect do |object_type|
          object_type.sql.qualified_table_name
        end
      end

      self.modules = Proc.new do
        require 'domgen'
        Domgen.repository_by_name(repository_key).data_modules.collect{|data_module| data_module.name}
      end

      task "dbt:#{key}:load_config" => load_task_name
      task "dbt:#{key}:pre_build" => generate_task_name
    end

    # Enable db doc support. Assume that all the directories in up/down will have documentation and
    # will generate relative to specified directory.
    def enable_db_doc(target_directory)
      task "dbt:#{key}:db_doc"
      task "dbt:#{key}:pre_build" => ["dbt:#{key}:db_doc"]

      (up_dirs + down_dirs).each do |relative_dir_name|
        dirs_for_database(relative_dir_name).each do |dir|
          task "dbt:#{key}:db_doc" => DbTasks::DbDoc.define_doc_tasks(dir, "#{target_directory}/#{relative_dir_name}")
        end
      end
    end
  end

  @@defined_init_tasks = false
  @@database_driver_hooks = []
  @@databases = {}
  @@configurations = {}

  def self.init(database_key, &block)
    setup_connection(config_key(database_key), &block)
  end

  def self.init_msdb(&block)
    setup_connection(:msdb, &block)
  end

  def self.add_database_driver_hook(&block)
    @@database_driver_hooks << block
  end

  def self.add_database(database_key, options = {})
    self.define_basic_tasks

    raise "Database with key #{database_key} already defined." if @@databases.has_key?(database_key)

    database = DatabaseDefinition.new(database_key, options)
    @@databases[database_key] = database

    yield database if block_given?

    define_tasks_for_database(database)
  end

  def self.filter_database_name(sql, pattern, target_database_config_key, optional = true)
    return sql if optional && self.configurations[target_database_config_key].nil?
    sql.gsub(pattern, get_config(target_database_config_key)['database'])
  end

  def self.dump_tables_to_fixtures(tables, fixture_dir)
    tables.each do |table_name|
      File.open("#{fixture_dir}/#{table_name}.yml", 'wb') do |file|
        puts("Dumping #{table_name}\n")
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
        i = 0
        dump_class.find_by_sql(sql).collect do |record|
          records["r#{i += 1}"] = record.attributes
        end

        file.write records.to_yaml
      end
    end
  end

  def self.load_modules_fixtures(database_key, module_name)
    database = database_for_key(database_key)
    init(database.key) do
      load_fixtures(database, module_name)
    end

  end

  private

  IMPORT_RESUME_AT_ENV_KEY = "IMPORT_RESUME_AT"

  def self.partial_import_completed?
    !!ENV[IMPORT_RESUME_AT_ENV_KEY]
  end

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
      banner('Dropping database', database.key)
      drop(database)
    end

    # Database creation

    task "dbt:#{database.key}:pre_build" => ['dbt:pre_build']

    desc "Create the #{database.key} database."
    task "dbt:#{database.key}:create" => ["dbt:#{database.key}:pre_build", "dbt:#{database.key}:load_config"] do
      banner('Creating database', database.key)
      init(database.key) do
        perform_create_action(database, :up)
        perform_create_action(database, :finalize)
      end
    end

    # Data set loading etc
    database.datasets.each do |dataset_name|
      desc "Loads #{dataset_name} data"
      task "dbt:#{database.key}:datasets:#{dataset_name}" => ["dbt:#{database.key}:load_config"] do
        banner("Loading Dataset #{dataset_name}", database.key)
        init(database.key) do
          database.modules.each do |module_name|
            load_dataset(database, module_name, dataset_name)
          end
        end
      end
    end

    # Import tasks
    if database.enable_separate_import_task?
      database.imports.values.each do |imp|
        define_import_task("dbt:#{database.key}", imp, "contents")
      end
    end

    database.module_groups.values.each do |module_group|
      define_module_group_tasks(module_group)
    end

    if database.enable_import_task_as_part_of_create?
      database.imports.values.each do |imp|
        key = ""
        key = ":" + imp.key.to_s if imp.key != :default
        desc "Create the #{database.key} database by import."
        task "dbt:#{database.key}:create_by_import#{key}" => ["dbt:#{database.key}:load_config", "dbt:#{database.key}:pre_build"] do
          banner("Creating Database By Import", database.key)
          init(database.key) do
            perform_create_action(database, :up) unless partial_import_completed?
            perform_import_action(imp, false, nil)
            perform_create_action(database, :finalize)
          end
        end
      end
    end

    if database.backup?
      desc "Perform backup of #{database.key} database"
      task "dbt:#{database.key}:backup" => ["dbt:#{database.key}:load_config"] do
        backup(database)
      end
    end

    if database.restore?
      desc "Perform restore of #{database.key} database"
      task "dbt:#{database.key}:restore" => ["dbt:#{database.key}:load_config"] do
        restore(database)
      end
    end
  end

  def self.define_module_group_tasks(module_group)
    database = module_group.database
    desc "Up the #{module_group.key} module group in the #{database.key} database."
    task "dbt:#{database.key}:#{module_group.key}:up" => ["dbt:#{database.key}:load_config", "dbt:#{database.key}:pre_build"] do
      banner("Upping module group '#{module_group.key}'", database.key)
      init(database.key) do
        database.modules.each do |module_name|
          schema_name = database.schema_name_for_module(module_name)
          next unless module_group.modules.include?(schema_name)
          create_module(database, module_name, schema_name, :up)
          create_module(database, module_name, schema_name, :finalize)
        end
      end
    end

    desc "Down the #{module_group.key} schema group in the #{database.key} database."
    task "dbt:#{database.key}:#{module_group.key}:down" => ["dbt:#{database.key}:load_config", "dbt:#{database.key}:pre_build"] do
      banner("Downing module group '#{module_group.key}'", database.key)
      init(database.key) do
        database.modules.reverse.each do |module_name|
          schema_name = database.schema_name_for_module(module_name)
          next unless module_group.modules.include?(schema_name)
          process_module(database, module_name, :down)
        end
        schema_2_module = {}
        database.modules.each do |module_name|
          schema_name = database.schema_name_for_module(module_name)
          (schema_2_module[schema_name] ||= []) << module_name
        end
        module_group.modules.reverse.each do |schema_name|
          drop_module_group(database, schema_name, schema_2_module[schema_name])
          tables = schema_2_module[schema_name].each { |module_name| database.table_ordering(module_name) }.flatten
          drop_schema(schema_name, tables)
        end
      end
    end

    database.imports.values.each do |imp|
      import_modules = imp.modules.select do |module_name|
        module_group.modules.include?(database.schema_name_for_module(module_name))
      end
      if module_group.import_enabled? && !import_modules.empty?
        description = "contents of the #{module_group.key} module group"
        define_import_task("dbt:#{database.key}:#{module_group.key}", imp, description, module_group)
      end
    end
  end

  def self.import(database, module_name, import_dir, reindex, shrink, should_perform_delete)
    ordered_tables = database.table_ordering(module_name)

    # check the import configuration is set
    get_config(config_key(database.key, "import"))

    # Iterate over module in dependency order doing import as appropriate
    # Note: that tables with initial fixtures are skipped
    tables = ordered_tables.reject do |table|
      fixture_for_creation(database, module_name, table)
    end
    if should_perform_delete && !partial_import_completed?
      tables.reverse.each do |table|
        info("Deleting #{table}")
        run_import_sql(database, table, "DELETE FROM @@TARGET@@.@@TABLE@@")
      end
    end

    tables.each do |table|
      if ENV[IMPORT_RESUME_AT_ENV_KEY] == table
        run_import_sql(database, table, "DELETE FROM @@TARGET@@.@@TABLE@@")
        ENV[IMPORT_RESUME_AT_ENV_KEY] = nil
      end
      if !partial_import_completed?
        perform_import(database, module_name, table, import_dir)
        if reindex
          info("Reindexing #{table}")
          run_import_sql(database, table, "DBCC DBREINDEX (N'@@TARGET@@.@@TABLE@@', '', 0) WITH NO_INFOMSGS")
        end
      end
    end

    if reindex && ENV[IMPORT_RESUME_AT_ENV_KEY].nil?
      if shrink
        # We are shrinking the database in case any of the import scripts created tables/columns and dropped them
        # later. This would leave large chunks of empty space in the underlying files. However it has to be done before
        # we reindex otherwise the indexes will be highly fragmented.
        info("Shrinking database")
        run_import_sql(database, nil, "DBCC SHRINKDATABASE(N'@@TARGET@@', 10, NOTRUNCATE) WITH NO_INFOMSGS")
        run_import_sql(database, nil, "DBCC SHRINKDATABASE(N'@@TARGET@@', 10, TRUNCATEONLY) WITH NO_INFOMSGS")

        tables.each do |table|
          info("Reindexing #{table}")
          run_import_sql(database, table, "DBCC DBREINDEX (N'@@TARGET@@.@@TABLE@@', '', 0) WITH NO_INFOMSGS")
        end
      end

      info("Updating statistics")
      run_import_sql(database, nil, "EXEC @@TARGET@@.dbo.sp_updatestats")

      # This updates the usage details for the database. i.e. how much space is take for each index/table 
      info("Updating usage statistics")
      run_import_sql(database, nil, "DBCC UPDATEUSAGE(N'@@TARGET@@') WITH NO_INFOMSGS, COUNT_ROWS")
    end
  end

  def self.backup(database)
    banner("Backing up Database", database.key)
    init(database.key) do
      physical_name = physical_database_name(database.key)
      db.select_database(nil)
      registry_key = instance_registry_key(database.key)
      sql = <<SQL
  DECLARE @BackupDir VARCHAR(400)
  EXEC master.dbo.xp_regread @rootkey='HKEY_LOCAL_MACHINE',
    @key='SOFTWARE\\Microsoft\\Microsoft SQL Server\\#{registry_key}\\MSSQLServer',
    @value_name='BackupDirectory',
    @value=@BackupDir OUTPUT
  IF @BackupDir IS NULL RAISERROR ('Unable to locate BackupDirectory registry key', 16, 1) WITH SETERROR
  DECLARE @BackupName VARCHAR(500)
  SET @BackupName = @BackupDir + '\\#{physical_name}.bak'

BACKUP DATABASE [#{physical_name}] TO DISK = @BackupName
WITH FORMAT, INIT, NAME = N'POST_CI_BACKUP', SKIP, NOREWIND, NOUNLOAD, STATS = 10
SQL
      db.execute(sql)
    end
  end

  def self.restore(database)
    banner("Restoring Database", database.key)
    init(database.key) do
      physical_name = physical_database_name(database.key)
      db.select_database(nil)
      registry_key = instance_registry_key(database.key)
      sql = <<SQL
  DECLARE @TargetDatabase VARCHAR(400)
  DECLARE @SourceDatabase VARCHAR(400)
  SET @TargetDatabase = '#{physical_name}'
  SET @SourceDatabase = '#{restore_from(database.key)}'

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
      db.execute("ALTER DATABASE [#{physical_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
      db.execute(sql)
    end
  end

  def self.drop(database)
    init_msdb
    db.select_database(nil)
    physical_name = physical_database_name(database.key)

    sql = if force_drop?(database.key)
      <<SQL
GO
  IF EXISTS
    ( SELECT *
      FROM  sys.master_files
      WHERE state = 0 AND db_name(database_id) = '#{physical_name}')
    ALTER DATABASE [#{physical_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
SQL
    else
      ''
    end

    sql << <<SQL
GO
  IF EXISTS
    ( SELECT *
      FROM  sys.master_files
      WHERE state = 0 AND db_name(database_id) = '#{physical_name}')
    DROP DATABASE [#{physical_name}]
GO
SQL
    run_filtered_sql_batch(database, sql)
  end

  def self.create_module(database, module_name, schema_name, mode)
    db.create_schema(schema_name)
    process_module(database, module_name, mode)
  end

  def self.define_import_task(prefix, imp, description, module_group = nil)
    is_default_import = imp.key == :default
    desc_prefix = is_default_import ? 'Import' : "#{imp.key.to_s.capitalize} import"

    task_name = is_default_import ? :import : :"import:#{imp.key}"
    desc "#{desc_prefix} #{description} of the #{imp.database.key} database."
    task "#{prefix}:#{task_name}" => ["dbt:#{imp.database.key}:load_config"] do
      banner("Importing Database#{is_default_import ? '' :" (#{imp.key})"}", imp.database.key)
      init(imp.database.key) do
        perform_import_action(imp, true, module_group)
      end
    end
  end

  def self.perform_create_action(database, mode)
    database.modules.each_with_index do |module_name, idx|
      create_database(database) if (idx == 0 && mode == :up)
      schema_name = database.schema_name_for_module(module_name)
      create_module(database, module_name, schema_name, mode)
    end
  end

  def self.collect_files(directories)

    index = []
    files = []

    directories.each do |dir|

      index_file = File.join(dir, DbTasks::Config.index_file_name)
      index_entries =
        File.exists?(index_file) ? File.new(index_file).readlines.collect { |filename| filename.strip } : []
      index_entries.each do |e|
        exists = false
        directories.each do |d|
          if File.exists?(File.join(d, e))
            exists = true
            break
          end
        end
        raise "A specified index entry does not exist on the disk #{e}" unless exists
      end

      index += index_entries     

      if File.exists?(dir)
        files += Dir["#{dir}/*.sql"]
      end
      
    end

    file_map = {}

    files.each do |filename|
      basename =  File.basename(filename)
      file_map[basename] = (file_map[basename] || []) + [filename]
    end
    duplicates = file_map.reject { |basename, filenames| filenames.size == 1 }.values

    if !duplicates.empty?
      raise "Files with duplicate basename not allowed.\n\t#{duplicates.collect{|filenames| filenames.join("\n\t")}.join("\n\t")}"
    end

    files.sort! do |x, y|
      x_index = index.index(File.basename(x))
      y_index = index.index(File.basename(y))
      if x_index.nil? && y_index.nil?
         File.basename(x) <=> File.basename(y)
      elsif x_index.nil? && !y_index.nil?
        1
      elsif y_index.nil? && !x_index.nil?
        -1
      else
        x_index <=> y_index
      end
    end

    files
  end

  def self.perform_import_action(imp, should_perform_delete, module_group)
    if module_group.nil?
      imp.pre_import_dirs.each do |dir|
        files = collect_files(imp.database.dirs_for_database(dir))
        run_sql_files(imp.database, dir_display_name(dir), files, true)
      end unless partial_import_completed?
    end
    imp.modules.each do |module_key|
      if module_group.nil? || module_group.modules.include?(module_key)
        import(imp.database, module_key, imp.dir, imp.reindex?, imp.shrink?, should_perform_delete)
      end
    end
    if partial_import_completed?
      raise "Partial import unable to be completed as bad table name supplied #{ENV[IMPORT_RESUME_AT_ENV_KEY]}"
    end
    if module_group.nil?
      imp.post_import_dirs.each do |dir|
        files = collect_files(imp.database.dirs_for_database(dir))
        run_sql_files(imp.database, dir_display_name(dir), files, true)
      end
    end
  end

  def self.dir_display_name(dir)
    (dir == '.' ? 'Base' : dir.humanize)
  end

  def self.define_basic_tasks
    if !@@defined_init_tasks
      task "dbt:load_config" do
        @@database_driver_hooks.each do |database_hook|
          database_hook.call
        end
        self.configurations = YAML::load(ERB.new(IO.read(DbTasks::Config.config_filename)).result)
      end

      task "dbt:pre_build"

      @@defined_init_tasks = true
    end
  end

  def self.config_key(database_key, env = DbTasks::Config.environment)
    database_key.to_s == DbTasks::Config.default_database.to_s ? env : "#{database_key}_#{env}"
  end

  def self.run_import_sql(database, table, sql, script_file_name = nil, print_dot = false)
    sql = filter_sql(sql, database.filters)
    sql = sql.gsub(/@@TABLE@@/, table) if table
    sql = filter_database_name(sql, /@@SOURCE@@/, config_key(database.key, "import"))
    sql = filter_database_name(sql, /@@TARGET@@/, config_key(database.key))
    db.select_database(nil)
    run_sql_batch(sql, script_file_name, print_dot)
    db.select_database(physical_database_name(database.key))
  end

  def self.generate_standard_import_sql(table)
    sql = "INSERT INTO @@TARGET@@.#{table}("
    columns = db.column_names_for_table(table)
    sql += columns.join(', ')
    sql += ")\n  SELECT "
    sql += columns.collect { |c| c == '[BatchID]' ? "0" : c }.join(', ')
    sql += " FROM @@SOURCE@@.#{table}\n"
    sql
  end

  def self.perform_standard_import(database, table)
    run_import_sql(database, table, generate_standard_import_sql(table))
  end

  def self.perform_import(database, module_name, table, import_dir)
    identity_insert_sql = db.get_identity_insert_sql(table, true)
    run_import_sql(database, table, identity_insert_sql) if identity_insert_sql

    fixture_file = fixture_for_import(database, module_name, table, import_dir)
    sql_file = sql_for_import(database, module_name, table, import_dir)
    is_sql = !fixture_file && sql_file

    info("Importing #{table} (By #{fixture_file ? 'F' : is_sql ? 'S' : "D"})")
    if fixture_file
      load_fixture(table, fixture_file)
    elsif is_sql
      run_import_sql(database, table, IO.readlines(sql_file).join, sql_file, true)
    else
      perform_standard_import(database, table)
    end

    identity_insert_sql = db.get_identity_insert_sql(table, false)
    run_import_sql(database, table, identity_insert_sql) if identity_insert_sql
  end

  def self.setup_connection(config_key, &block)
    db.open(get_config(config_key), DbTasks::Config.log_filename)
    if block_given?
      yield
      db.close
    end
  end

  def self.create_database(database)
    return if no_create?(database.key)
    init_msdb
    drop(database)
    physical_name = physical_database_name(database.key)
    if database.version.nil?
      db_filename = physical_name
    else
      db_filename = "#{physical_name}_#{database.version.gsub(/\./, '_')}"
    end
    base_data_path = data_path(database.key)
    base_log_path = log_path(database.key)

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
SQL
    run_filtered_sql_batch(database, sql)

    db.select_database(physical_name)
    if !database.version.nil?
      sql = <<SQL
    EXEC sys.sp_addextendedproperty @name = N'DatabaseSchemaVersion', @value = N'#{database.version}'
SQL
      run_filtered_sql_batch(database, sql)
    end
  end

  def self.process_module(database, module_name, mode)
    dirs = mode == :up ? database.up_dirs : mode == :down ? database.down_dirs : database.finalize_dirs
    dirs.each do |dir|
      files = collect_files(dirs_for_module(database, module_name, dir))
      run_sql_files(database, "#{'%-10s' % "#{module_name}:" } #{dir_display_name(dir)}", files, false)
    end
    load_fixtures(database, module_name) if mode == :up
  end

  def self.load_fixtures(database, module_name)
    load_fixtures_from_dirs(database, module_name, dirs_for_module(database, module_name, 'fixtures'))
  end

  def self.load_dataset(database, module_name, dataset_name)
    load_fixtures_from_dirs(database, module_name, dirs_for_module(database, module_name, "datasets/#{dataset_name}"))
  end

  def self.load_fixtures_from_dirs(database, module_name, dirs)
    fixtures = {}
    database.table_ordering(module_name).each do |table_name|
      dirs.each do |dir|
        filename = "#{dir}/#{table_name}.yml"
        fixtures[table_name] = filename if File.exists?(filename)
      end
    end

    database.table_ordering(module_name).reverse.each do |table_name|
      db.execute("DELETE FROM #{table_name}") if fixtures[table_name]
    end

    database.table_ordering(module_name).each do |table_name|
      filename = fixtures[table_name]
      next unless filename
      info("Loading fixture: #{table_name}")
      load_fixture(table_name, filename)
    end
  end

  def self.load_fixture(table_name, filename)
    yaml = YAML::load(IO.read(filename))
    # Skip empty files
    next unless yaml
    # NFI
    yaml_value =
      if yaml.respond_to?(:type_id) && yaml.respond_to?(:value)
        yaml.value
      else
        [yaml]
      end

    yaml_value.each do |fixture|
      raise "Bad data for #{table_name} fixture named #{fixture}" unless fixture.respond_to?(:each)
      fixture.each do |name, data|
        raise "Bad data for #{table_name} fixture named #{name} (nil)" unless data

        column_names = data.keys.collect { |column_name| db.quote_column_name(column_name) }
        value_list = data.values.collect { |value| db.quote_value(value).gsub('[^\]\\n', "\n").gsub('[^\]\\r', "\r") }
        db.execute("INSERT INTO #{table_name} (#{column_names.join(', ')}) VALUES (#{value_list.join(', ')})")
      end
    end
  end

  def self.run_sql_batch(sql, script_file_name, print_dot)
    sql.gsub(/\r/, '').split(/(\s|^)GO(\s|$)/).reject { |q| q.strip.empty? }.each_with_index do |ddl, index|
      $stdout.putc '.' if print_dot
      begin
        db.execute(ddl)
      rescue
        if script_file_name.nil? || index.nil?
          raise $!
        else
          raise "An error occurred while trying to execute batch ##{index + 1} of #{File.basename(script_file_name)}:\n#{$!}"
        end
      end
    end
    $stdout.putc "\n" if print_dot
  end

  def self.get_config(config_key)
    c = self.configurations[config_key.to_s]
    raise "Missing config for #{config_key}" unless c
    c
  end

  def self.configurations
    @@configurations
  end

  def self.configurations=(configurations)
    @@configurations = configurations
  end

  def self.run_filtered_sql_batch(database, sql, script_file_name = nil)
    sql = filter_sql(sql, database.filters)
    run_sql_batch(sql, script_file_name, false)
  end

  def self.filter_sql(sql, filters)
    filters.each do |filter|
      sql = filter.call(sql)
    end
    sql
  end

  def self.run_sql_files(database, label, files, is_import)
    files.each do |filename|
      run_sql_file(database, label, filename, is_import)
    end
  end

  def self.run_sql_file(database, label, filename, is_import)
    info("#{label}: #{File.basename(filename)}")
    sql = IO.readlines(filename).join
    if is_import
      run_import_sql(database, nil, sql, filename)
    else
      run_filtered_sql_batch(database, sql, filename)
    end
  end

  def self.dirs_for_module(database, module_name, subdir = nil)
    database.search_dirs.map { |d| "#{d}/#{module_name}#{ subdir ? "/#{subdir}" : ''}" }
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

  def self.banner(message, database_key)
    info("**** #{message}: (Database: #{database_key}, Environment: #{DbTasks::Config.environment}) ****")
  end

  def self.info(message)
    puts message
  end

  def self.no_create?(database_key)
    true == config_value(database_key, "no_create", true)
  end

  def self.force_drop?(database_key)
    true == config_value(database_key, "force_drop", true)
  end

  def self.data_path(database_key)
    config_value(database_key, "data_path", true)
  end

  def self.log_path(database_key)
    config_value(database_key, "log_path", true)
  end

  def self.restore_from(database_key)
    config_value(database_key, "restore_from", false)
  end

  def self.instance_registry_key(database_key)
    config_value(database_key, "instance_registry_key", false)
  end

  def self.physical_database_name(database_key)
    config_value(database_key, "database", false)
  end

  def self.config_value(database_key, config_param_name, allow_nil)
    config_key = config_key(database_key)
    value = get_config(config_key)[config_param_name]
    raise "Unable to locate configuration value named #{config_param_name} in section #{config_key}" if !allow_nil && value.nil?
    value
  end

  def self.db
    DbDriver.new
  end

  class DbDriver
    def quote_column_name(column_name)
      ActiveRecord::Base.connection.quote_column_name(column_name)
    end

    def quote_value(value)
      ActiveRecord::Base.connection.quote(value)
    end

    def execute(sql)
      ActiveRecord::Base.connection.execute(sql, nil)
    end

    def select_values(sql)
      ActiveRecord::Base.connection.select_values(sql, nil)
    end

    def create_schema(schema_name)
      if ActiveRecord::Base.connection.select_all("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
        execute("CREATE SCHEMA [#{schema_name}]")
      end
    end

    def drop_schema(schema_name, tables)
      database_objects("SQL_STORED_PROCEDURE", schema_name).each { |name| execute("DROP PROCEDURE #{name}") }
      database_objects("SQL_SCALAR_FUNCTION", schema_name).each { |name| execute("DROP FUNCTION #{name}") }
      database_objects("SQL_INLINE_TABLE_VALUED_FUNCTION", schema_name).each { |name| execute("DROP FUNCTION #{name}") }
      database_objects("SQL_TABLE_VALUED_FUNCTION", schema_name).each { |name| execute("DROP FUNCTION #{name}") }
      database_objects("VIEW", schema_name).each { |name| execute("DROP VIEW #{name}") }
      tables.reverse.each do |table|
        execute("DROP TABLE #{t}")
      end
      execute("DROP SCHEMA #{schema_name}")
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

    def column_names_for_table(table)
      ActiveRecord::Base.connection.columns(table).collect { |c| quote_column_name(c.name) }
    end

    def open(config, log_filename)
      require 'active_record'
      ActiveRecord::Base.colorize_logging = false
      ActiveRecord::Base.establish_connection(config)
      FileUtils.mkdir_p File.dirname(log_filename)
      ActiveRecord::Base.logger = Logger.new(File.open(log_filename, 'a'))
      ActiveRecord::Migration.verbose = ENV["VERBOSE"] ? ENV["VERBOSE"] == "true" : false
    end

    def close
      ActiveRecord::Base.connection.disconnect! if ActiveRecord::Base.connection && ActiveRecord::Base.connection.active?
    end

    def has_identity_column(table)
      ActiveRecord::Base.connection.columns(table).each do |c|
        return true if c.identity == true
      end
      false
    end

    def get_identity_insert_sql(table, value)
      if has_identity_column(table)
        "SET IDENTITY_INSERT @@TARGET@@.@@TABLE@@ #{value ? 'ON' : 'OFF'}"
      else
        nil
      end
    end

    def select_database(database_name)
      if database_name.nil?
        ActiveRecord::Base.connection.execute "USE [msdb]"
      else
        ActiveRecord::Base.connection.execute "USE [#{database_name}]"
      end
    end
  end
end
