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

class Dbt

  class Config

    @datasets_dir_name = nil
    @repository_config_file = nil
    @default_up_dirs = nil
    @default_down_dirs = nil
    @default_finalize_dirs = nil
    @default_pre_create_dirs = nil
    @default_post_create_dirs = nil
    @default_pre_import_dirs = nil
    @default_post_import_dirs = nil
    @index_file_name = nil
    @default_import = nil
    @fixture_dir_name = nil
    @environment = nil
    @driver = nil
    @default_migrations_dir_name = nil
    @default_database = nil
    @task_prefix = nil

    class << self
      attr_writer :environment

      def environment
        @environment || 'development'
      end

      attr_writer :task_prefix

      def task_prefix
        @task_prefix || 'dbt'
      end

      attr_writer :driver

      def driver
        @driver || 'Mssql'
      end

      attr_writer :default_no_create

      def default_no_create?
        @default_no_create.nil? ? false : @default_no_create
      end

      attr_writer :default_database

      def default_database
        @default_database || :default
      end

      attr_writer :default_import

      def default_import
        @default_import || :default
      end

      # config_file is where the yaml config file is located
      attr_writer :config_filename

      def config_filename
        raise "config_filename not specified" unless @config_filename
        @config_filename
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

      attr_writer :default_pre_create_dirs

      def default_pre_create_dirs
        @default_pre_create_dirs || ['db-hooks/pre']
      end

      attr_writer :default_post_create_dirs

      def default_post_create_dirs
        @default_post_create_dirs || ['db-hooks/post']
      end

      attr_writer :default_down_dirs

      def default_down_dirs
        @default_down_dirs || ['down']
      end

      attr_writer :index_file_name

      def index_file_name
        @index_file_name || 'index.txt'
      end

      attr_writer :fixture_dir_name

      def fixture_dir_name
        @fixture_dir_name || 'fixtures'
      end

      attr_writer :datasets_dir_name

      def datasets_dir_name
        @datasets_dir_name || 'datasets'
      end

      attr_writer :default_migrations_dir_name

      def default_migrations_dir_name
        @default_migrations_dir_name || 'migrations'
      end

      attr_writer :repository_config_file

      def repository_config_file
        @repository_config_file || 'repository.yml'
      end
    end
  end

  DatabaseNameFilter = ::Struct.new('DatabaseNameFilter', :pattern, :database_key, :optional)
  PropertyFilter = ::Struct.new('PropertyFilter', :pattern, :value)

  module FilterContainer
    def add_filter(&block)
      self.filters << block
    end

    def add_database_name_filter(pattern, database_key, optional = false)
      self.filters << DatabaseNameFilter.new(pattern, database_key, optional)
    end

    # Filter the SQL files replacing specified pattern with specified value
    def add_property_filter(pattern, value)
      self.filters << PropertyFilter.new(pattern, value)
    end

    # Makes the import scripts support statements such as
    #   ASSERT_ROW_COUNT(1)
    #   ASSERT_ROW_COUNT(SELECT COUNT(*) FROM Foo)
    #   ASSERT_UNCHANGED_ROW_COUNT()
    #   ASSERT(@Id IS NULL)
    #
    def add_import_assert_filters
      @add_import_assert_filters = true
    end

    def add_import_assert_filters?
      @add_import_assert_filters.nil? ? false : @add_import_assert_filters
    end

    def add_database_environment_filter
      @add_database_environment_filter = true
    end

    def add_database_environment_filter?
      @add_database_environment_filter.nil? ? false : @add_database_environment_filter
    end


    def filters
      @filters ||= []
    end

    def expanded_filters
      filters = []
      if add_import_assert_filters?
        filters << Proc.new do |sql|
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

      if add_database_environment_filter?
        filters << Proc.new do |sql|
            sql.gsub(/@@ENVIRONMENT@@/, Dbt::Config.environment.to_s)
          end
      end

      self.filters.each do |filter|
        if filter.is_a?(PropertyFilter)
          filters << Proc.new do |sql|
            sql.gsub(filter.pattern, filter.value)
          end
        elsif filter.is_a?(DatabaseNameFilter)
          filters << Proc.new do |sql|
            Dbt.filter_database_name(sql, filter.pattern, Dbt.config_key(filter.database_key), filter.optional)
          end
        else
          filters << filter
        end
      end

      filters
    end
  end

  class ImportDefinition
    include FilterContainer

    def initialize(database, key, options)
      @database = database
      @key = key
      @modules = options[:modules]
      @dir = options[:dir]
      @reindex = options[:reindex]
      @pre_import_dirs = options[:pre_import_dirs]
      @post_import_dirs = options[:post_import_dirs]
    end

    attr_accessor :database
    attr_accessor :key

    def modules
      @modules || database.modules
    end

    def dir
      @dir || "import"
    end

    # TODO: Move to specific DbConfig
    attr_writer :reindex

    def reindex?
      @reindex.nil? ? true : @reindex
    end

    # TODO: Move to specific DbConfig
    attr_writer :shrink

    def shrink?
      @shrink.nil? ? false : @shrink
    end

    attr_writer :pre_import_dirs

    def pre_import_dirs
      @pre_import_dirs || Dbt::Config.default_pre_import_dirs
    end

    attr_writer :post_import_dirs

    def post_import_dirs
      @post_import_dirs || Dbt::Config.default_post_import_dirs
    end

    def validate
      self.modules.each do |module_key|
        if !database.modules.include?(module_key.to_s)
          raise "Module #{module_key} in import #{self.key} does not exist in database module list #{self.database.modules.inspect}"
        end
      end
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

    attr_writer :modules

    def modules
      raise "Missing modules configuration for module_group #{key}" unless @modules
      @modules
    end

    def import_enabled?
      @import_enabled.nil? ? false : @import_enabled
    end

    def validate
      self.modules.each do |module_key|
        unless database.modules.include?(module_key.to_s)
          raise "Module #{module_key} in module group #{self.key} does not exist in database module list #{self.database.modules.inspect}"
        end
      end
    end
  end

  class DatabaseDefinition
    include FilterContainer

    def initialize(key, options)
      @key = key
      @migrations = options[:migrations]
      @backup = options[:backup]
      @restore = options[:restore]
      @datasets = options[:datasets]
      raise "schema_overrides should be derived from repository.yml and not directly specified." if options[:schema_overrides]
      raise "modules should be derived from repository.yml and not directly specified." if options[:modules]
      @resource_prefix = nil
      @up_dirs = nil
      @down_dirs = nil
      @finalize_dirs = nil
      @pre_create_dirs = nil
      @post_create_dirs = nil
      @search_dirs = nil
      @migrations_dir_name = nil

      @imports = {}
      imports_config = options[:imports]
      if imports_config
        imports_config.keys.each do |import_key|
          add_import(import_key, imports_config[import_key])
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

    def add_import(import_key, import_config)
      @imports[import_key] = ImportDefinition.new(self, import_key, import_config)
    end

    def validate
      @imports.values.each {|d| d.validate}
      @module_groups.values.each {|d| d.validate}
    end

    # symbolic name of database
    attr_reader :key

    # List of modules to import
    attr_reader :imports

    # List of module_groups configs
    attr_reader :module_groups

    attr_writer :modules

    attr_writer :migrations

    def enable_migrations?
      @migrations.nil? ? false : !!@migrations
    end

    attr_writer :migrations_applied_at_create

    def assume_migrations_applied_at_create?
      @migrations_applied_at_create.nil? ? true : @migrations_applied_at_create
    end

    attr_writer :rake_integration

    def enable_rake_integration?
      @rake_integration.nil? ? true : @rake_integration
    end

    def task_prefix
      raise "task_prefix invoked" unless enable_rake_integration?
      "#{Dbt::Config.task_prefix}#{Dbt.default_database?(self.key) ? '' : ":#{self.key}"}"
    end

    # List of modules to process for database
    def modules
      @modules = @modules.call if !@modules.nil? && @modules.is_a?(Proc)
      @modules
    end

    # Database version. Stuffed as an extended property and used when creating filename.
    attr_accessor :version

    attr_writer :pre_create_dirs

    def pre_create_dirs
      @pre_create_dirs || Dbt::Config.default_pre_create_dirs
    end

    attr_writer :post_create_dirs

    def post_create_dirs
      @post_create_dirs || Dbt::Config.default_post_create_dirs
    end

    attr_writer :migrations_dir_name

    def migrations_dir_name
      @migrations_dir_name || Dbt::Config.default_migrations_dir_name
    end

    # If there is a resource path then we are loading from within the jar
    # so we should not attempt to scan search directories
    def load_from_classloader?
      !!@resource_prefix
    end

    attr_accessor :resource_prefix

    attr_writer :search_dirs

    def search_dirs
      @search_dirs || Dbt::Config.default_search_dirs
    end

    def dirs_for_database(subdir)
      search_dirs.map { |d| "#{d}/#{subdir}" }
    end

    attr_writer :up_dirs

    # Return the list of dirs to process when "upping" module
    def up_dirs
      @up_dirs || Dbt::Config.default_up_dirs
    end

    attr_writer :down_dirs

    # Return the list of dirs to process when "downing" module
    def down_dirs
      @down_dirs || Dbt::Config.default_down_dirs
    end

    attr_writer :finalize_dirs

    # Return the list of dirs to process when finalizing module.
    # i.e. Getting database ready for use. Often this is the place to add expensive triggers, constraints and indexes
    # after the import
    def finalize_dirs
      @finalize_dirs || Dbt::Config.default_finalize_dirs
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
      @backup.nil? ? false : @backup
    end

    attr_writer :restore

    # Should the a restore task be defined for database?
    def restore?
      @restore.nil? ? false : @restore
    end

    attr_writer :schema_overrides

    # Map of module => schema overrides
    # i.e. What database schema is created for a specific module
    def schema_overrides
      @schema_overrides || {}
    end

    attr_writer :table_map

    def table_map
      @table_map || {}
    end

    def schema_name_for_module(module_name)
      schema_overrides[module_name] || module_name
    end

    def table_ordering(module_name)
      tables = table_map[module_name.to_s]
      raise "No tables defined for module #{module_name}" unless tables
      tables
    end

    # Enable domgen support. Assume the database is associated with a single repository
    # definition, a single task to generate sql etc.
    def enable_domgen(repository_key, load_task_name, generate_task_name)
      task "#{task_prefix}:load_config" => load_task_name
      task "#{task_prefix}:pre_build" => generate_task_name

      desc "Verify constraints on database."
      task "#{task_prefix}:verify_constraints" => ["#{task_prefix}:load_config"] do
        Dbt.banner("Verifying database", key)
        Dbt.init_database(key) do
          failed_constraints = []
          Domgen.repository_by_name(repository_key).data_modules.select { |data_module| data_module.sql? }.each do |data_module|
            failed_constraints += Dbt.db.query("EXEC #{data_module.sql.schema}.spCheckConstraints")
          end
          if failed_constraints.size > 0
            error_message = "Failed Constraints:\n#{failed_constraints.collect do |row|
              "\t#{row['ConstraintName']} on #{row['SchemaName']}.#{row['TableName']}"
            end.join("\n")}"
            raise error_message
          end
        end
        Dbt.banner("Database verified", key)
      end
    end

    # Enable db doc support. Assume that all the directories in up/down will have documentation and
    # will generate relative to specified directory.
    def enable_db_doc(target_directory)
      task "#{task_prefix}:db_doc"
      task "#{task_prefix}:pre_build" => ["#{task_prefix}:db_doc"]

      (up_dirs + down_dirs).each do |relative_dir_name|
        dirs_for_database(relative_dir_name).each do |dir|
          task "#{task_prefix}:db_doc" => Dbt::DbDoc.define_doc_tasks(dir, "#{target_directory}/#{relative_dir_name}")
        end
      end
    end
  end

  @@defined_init_tasks = false
  @@database_driver_hooks = []
  @@databases = {}
  @@configurations = {}
  @@configuration_data = {}

  def self.init_database(database_key, &block)
    setup_connection(database_key, false, &block)
  end

  def self.init_control_database(database_key, &block)
    setup_connection(database_key, true, &block)
  end

  def self.add_database_driver_hook(&block)
    @@database_driver_hooks << block
  end

  def self.add_database(database_key, options = {})
    raise "Database with key #{database_key} already defined." if @@databases.has_key?(database_key)

    database = DatabaseDefinition.new(database_key, options)
    @@databases[database_key] = database

    yield database if block_given?

    define_tasks_for_database(database) if database.enable_rake_integration?

    database
  end

  def self.define_database_package(database_key, buildr_project, options = {})
    database = database_for_key(database_key)
    package_dir = buildr_project._(:target, 'dbt')

    task "#{database.task_prefix}:package" => ["#{database.task_prefix}:prepare_fs"] do
      banner("Packaging Database Scripts", database.key)
      package_database(database, package_dir)
    end
    buildr_project.file("#{package_dir}/code" => "#{database.task_prefix}:package")
    buildr_project.file("#{package_dir}/data" => "#{database.task_prefix}:package")
    jar = buildr_project.package(:jar) do |j|
    end
    dependencies =
      ["org.jruby:jruby-complete:jar:#{JRUBY_VERSION}"] +
        Dbt.const_get("#{Dbt::Config.driver}DbConfig").jdbc_driver_dependencies

    #TODO: Buildr is too slow merging jars so lets try an ugly hack
    if false
      dependencies.each do |spec|
        jar.merge(Buildr.artifact(spec))
      end
    else
      dependencies.each do |spec|
        dependency = Buildr.artifact(spec)
        jar.enhance [dependency] do
          Buildr.unzip("#{package_dir}/code" => dependency.to_s).extract
        end
      end
    end
    jar.include "#{package_dir}/code", :as => '.'
    jar.include "#{package_dir}/data"
    jar.with :manifest => buildr_project.manifest.merge('Main-Class'=>'org.realityforge.dbt.dbtcli')
  end

  def self.filter_database_name(sql, pattern, config_key, optional = true)
    return sql if optional && self.configuration_data[config_key].nil?
    sql.gsub(pattern, configuration_for_key(config_key).catalog_name)
  end

  def self.dump_tables_to_fixtures(tables, fixture_dir)
    tables.each do |table_name|
      File.open(table_name_to_fixture_filename(fixture_dir, table_name), 'wb') do |file|
        puts("Dumping #{table_name}\n")
        const_name = :"DUMP_SQL_FOR_#{clean_table_name(table_name).gsub('.', '_')}"
        if Object.const_defined?(const_name)
          sql = Object.const_get(const_name)
        else
          sql = "SELECT * FROM #{table_name}"
        end

        records = YAML::Omap.new
        i = 0
        db.query(sql).each do |record|
          records["r#{i += 1}"] = record
        end

        file.write records.to_yaml
      end
    end
  end

  def self.load_modules_fixtures(database_key, module_name)
    database = database_for_key(database_key)
    init_database(database.key) do
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

  def self.configuration_for_database(database)
    configuration_for_key(config_key(database.key))
  end

  def self.define_tasks_for_database(database)
    self.define_basic_tasks
    task "#{database.task_prefix}:load_config" => ["#{Dbt::Config.task_prefix}:global:load_config"]

    # Database dropping

    desc "Drop the #{database.key} database."
    task "#{database.task_prefix}:drop" => ["#{database.task_prefix}:load_config"] do
      banner('Dropping database', database.key)
      drop(database)
    end

    # Database creation

    task "#{database.task_prefix}:pre_build" => ["#{Dbt::Config.task_prefix}:all:pre_build"]

    task "#{database.task_prefix}:prepare_fs" => ["#{database.task_prefix}:pre_build"] do
      load_database_config(database)
    end

    task "#{database.task_prefix}:prepare" => ["#{database.task_prefix}:load_config", "#{database.task_prefix}:prepare_fs"]

    desc "Create the #{database.key} database."
    task "#{database.task_prefix}:create" => ["#{database.task_prefix}:prepare"] do
      banner('Creating database', database.key)
      create(database)
    end

    # Data set loading etc
    database.datasets.each do |dataset_name|
      desc "Loads #{dataset_name} data"
      task "#{database.task_prefix}:datasets:#{dataset_name}" => ["#{database.task_prefix}:prepare"] do
        banner("Loading Dataset #{dataset_name}", database.key)
        load_datasets_for_modules( database, dataset_name )
      end
    end

    if database.enable_migrations?
      desc "Apply migrations to bring data to latest version"
      task "#{database.task_prefix}:migrate" => ["#{database.task_prefix}:prepare"] do
        banner("Migrating", database.key)
        migrate(database)
      end
    end

    # Import tasks
    if database.enable_separate_import_task?
      database.imports.values.each do |imp|
        define_import_task("#{database.task_prefix}", imp, "contents")
      end
    end

    database.module_groups.values.each do |module_group|
      define_module_group_tasks(module_group)
    end

    if database.enable_import_task_as_part_of_create?
      database.imports.values.each do |imp|
        key = ""
        key = ":" + imp.key.to_s unless default_import?(imp.key)
        desc "Create the #{database.key} database by import."
        task "#{database.task_prefix}:create_by_import#{key}" => ["#{database.task_prefix}:prepare"] do
          banner("Creating Database By Import", database.key)
          create_by_import(imp)
        end
      end
    end

    if database.backup?
      desc "Perform backup of #{database.key} database"
      task "#{database.task_prefix}:backup" => ["#{database.task_prefix}:load_config"] do
        banner("Backing up Database", database.key)
        backup(database)
      end
    end

    if database.restore?
      desc "Perform restore of #{database.key} database"
      task "#{database.task_prefix}:restore" => ["#{database.task_prefix}:load_config"] do
        banner("Restoring Database", database.key)
        restore(database)
      end
    end
  end

  def self.define_module_group_tasks(module_group)
    database = module_group.database
    desc "Up the #{module_group.key} module group in the #{database.key} database."
    task "#{database.task_prefix}:#{module_group.key}:up" => ["#{database.task_prefix}:prepare"] do
      banner("Upping module group '#{module_group.key}'", database.key)
      up_module_group(module_group)
    end

    desc "Down the #{module_group.key} schema group in the #{database.key} database."
    task "#{database.task_prefix}:#{module_group.key}:down" => ["#{database.task_prefix}:prepare"] do
      banner("Downing module group '#{module_group.key}'", database.key)
      down_module_group(module_group)
    end

    database.imports.values.each do |imp|
      import_modules = imp.modules.select { |module_name| module_group.modules.include?(module_name) }
      if module_group.import_enabled? && !import_modules.empty?
        description = "contents of the #{module_group.key} module group"
        define_import_task("#{database.task_prefix}:#{module_group.key}", imp, description, module_group)
      end
    end
  end

  def self.define_import_task(prefix, imp, description, module_group = nil)
    is_default_import = default_import?(imp.key)
    desc_prefix = is_default_import ? 'Import' : "#{imp.key.to_s.capitalize} import"

    task_name = is_default_import ? :import : :"import:#{imp.key}"
    desc "#{desc_prefix} #{description} of the #{imp.database.key} database."
    task "#{prefix}:#{task_name}" => ["#{imp.database.task_prefix}:prepare"] do
      banner("Importing Database#{is_default_import ? '' : " (#{imp.key})"}", imp.database.key)
      database_import(imp, module_group)
    end
  end

  def self.define_basic_tasks
    if !@@defined_init_tasks
      task "#{Dbt::Config.task_prefix}:global:load_config" do
        global_init
      end

      task "#{Dbt::Config.task_prefix}:all:pre_build"

      @@defined_init_tasks = true
    end
  end

  def self.execute_command(database, command)
    if "create" == command
      Dbt.create(database)
    elsif "drop" == command
      Dbt.drop(database)
    elsif "migrate" == command
      Dbt.migrate(database)
    elsif "restore" == command
      Dbt.restore(database)
    elsif "backup" == command
      Dbt.backup(database)
    elsif /^datasets:/ =~ command
      dataset_name = command[9,command.length]
      Dbt.load_datasets_for_modules(database, dataset_name)
    elsif /^import/ =~ command
      import_key = command[7,command.length]
      import_key = Dbt::Config.default_import.to_s if import_key.nil?
      database.imports.values.each do |imp|
        if imp.key.to_s == import_key
          database_import(imp, nil)
          return
        end
      end
      raise "Unknown import '#{import_key}'"
    elsif /^create_by_import/ =~ command
      import_key = command[17,command.length]
      import_key = Dbt::Config.default_import.to_s if import_key.nil?
      database.imports.values.each do |imp|
        if imp.key.to_s == import_key
          create_by_import(imp)
          return
        end
      end
      raise "Unknown import '#{import_key}'"
    else
      raise "Unknown command '#{command}'"
    end
  end

  def self.package_database(database, package_dir)
    rm_rf package_dir
    package_database_code(database, "#{package_dir}/code")
    package_database_data(database, "#{package_dir}/data")
  end

  def self.package_database_code(database, package_dir)
    mkdir_p package_dir
    valid_commands = ["create","drop"]
    valid_commands << "restore" if database.restore?
    valid_commands << "backup" if database.backup?
    if database.enable_separate_import_task?
      database.imports.values.each do |imp|
        command = "import"
        command = "#{command}:#{imp.key}" unless default_import?(imp.key)
        valid_commands << command
      end
    end
    if database.enable_import_task_as_part_of_create?
      database.imports.values.each do |imp|
        command = "create_by_import"
        command = "#{command}:#{imp.key}" unless default_import?(imp.key)
        valid_commands << command
      end
    end
    database.datasets.each do |dataset|
      valid_commands << "datasets:#{dataset}"
    end

    valid_commands << "migrate" if database.enable_migrations?

    mkdir_p "#{package_dir}/org/realityforge/dbt"
    File.open("#{package_dir}/org/realityforge/dbt/dbtcli.rb","w") do |f|
      f << <<TXT
require 'dbt'
require 'optparse'
require 'java'

Dbt::Config.driver = '#{Dbt::Config.driver}'
Dbt::Config.environment = 'production'
Dbt::Config.config_filename = 'config/database.yml'
VALID_COMMANDS=#{valid_commands.inspect}

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: dbtcli [OPTIONS] [COMMANDS]"
  opt.separator  ""
  opt.separator  "Commands: #{valid_commands.join(', ')}"
  opt.separator  ""
  opt.separator  "Options"

  opt.on("-e","--environment ENV","the database environment to use. Defaults to 'production'.") do |environment|
    Dbt::Config.environment = environment
  end

  opt.on("-c","--config-file CONFIG","the configuration file to use. Defaults to 'config/database.yml'.") do |config_filename|
    Dbt::Config.config_filename = config_filename
  end

  opt.on("-h","--help","help") do
    puts opt_parser
    java.lang.System.exit(53)
  end
end

begin
  opt_parser.parse!
rescue => e
  puts "Error: \#{e.message}"
  java.lang.System.exit(53)
end

ARGV.each do |command|
  unless VALID_COMMANDS.include?(command) || /^datasets:/ =~ command
    puts "Unknown command: \#{command}"
    java.lang.System.exit(42)
  end
end

if ARGV.length == 0
  puts "No command specified"
  java.lang.System.exit(31)
end

database = Dbt.add_database(:#{database.key}) do |database|
  database.version = #{database.version.inspect}
  database.resource_prefix = "data"
  database.datasets = %w(#{database.datasets.join(' ')})
TXT
      if database.add_import_assert_filters?
        f << "  database.add_import_assert_filters\n"
      end

      if database.add_database_environment_filter?
        f << "  database.add_database_environment_filter\n"
      end

      database.filters.each do |filter|
        if filter.is_a?(PropertyFilter)
          f << "  database.add_property_filter(#{filter.pattern.inspect}, #{filter.value.inspect})\n"
        elsif filter.is_a?(DatabaseNameFilter)
          f << "  database.add_database_name_filter(#{filter.pattern.inspect}, #{filter.database_key.inspect}, #{filter.optional.inspect})\n"
        else
          raise "Unsupported filter #{filter}"
        end
      end

      database.imports.each_pair do |import_key, definition|
        import_config = {
          :modules => definition.modules,
          :dir => definition.dir,
          :reindex => definition.reindex?,
          :shrink => definition.shrink?,
          :pre_import_dirs => definition.pre_import_dirs,
          :post_import_dirs => definition.post_import_dirs
        }
        f << "  database.add_import(:#{import_key}, #{import_config.inspect})\n"
      end

      f << <<TXT
  database.rake_integration = false
  database.migrations = #{database.enable_migrations?}
end

puts "Environment: \#{Dbt::Config.environment}"
puts "Config File: \#{Dbt::Config.config_filename}"
puts "Commands: \#{ARGV.join(' ')}"

Dbt.global_init
Dbt.load_database_config(database)

ARGV.each do |command|
  Dbt.execute_command(database, command)
end
TXT
    end
    sh "jrubyc --dir #{::Buildr::Util.relative_path(package_dir,Dir.pwd)} #{::Buildr::Util.relative_path(package_dir,Dir.pwd)}/org/realityforge/dbt/dbtcli.rb"
    cp_r Dir.glob("#{File.expand_path(File.dirname(__FILE__) + '/..')}/*"), package_dir
  end

  def self.cp_files_to_dir(files, target_dir)
    return if files.empty?
    mkdir_p target_dir
    cp_r files, target_dir
  end

  def self.package_database_data(database, package_dir)
    mkdir_p package_dir

    import_dirs = database.imports.values.collect { |i| i.dir }.sort.uniq
    dataset_dirs = database.datasets.collect{ |dataset| "#{Dbt::Config.datasets_dir_name}/#{dataset}"}
    dirs = database.up_dirs + database.down_dirs + database.finalize_dirs + [Dbt::Config.fixture_dir_name] + import_dirs + dataset_dirs
    database.modules.each do |module_name|
      dirs.each do |relative_dir_name|
        relative_module_dir = "#{module_name}/#{relative_dir_name}"
        target_dir = "#{package_dir}/#{module_name}/#{relative_dir_name}"
        actual_dirs = database.dirs_for_database(relative_module_dir)
        files = collect_files(actual_dirs)
        cp_files_to_dir(files, target_dir)
        generate_index(target_dir, files) unless import_dirs.include?(relative_dir_name)
        actual_dirs.each do |dir|
          if File.exist?(dir)
            if Dbt::Config.fixture_dir_name == relative_dir_name || dataset_dirs.include?(relative_dir_name)
              database.table_ordering(module_name).each do |table_name|
                cp_files_to_dir(Dir.glob("#{dir}/#{clean_table_name(table_name)}.yml"), target_dir)
              end
            else
              if import_dirs.include?(relative_dir_name)
                database.table_ordering(module_name).each do |table_name|
                  cp_files_to_dir(Dir.glob("#{dir}/#{clean_table_name(table_name)}.yml"), target_dir)
                  cp_files_to_dir(Dir.glob("#{dir}/#{clean_table_name(table_name)}.sql"), target_dir)
                end
              else
                cp_files_to_dir(Dir.glob("#{dir}/*.sql"), target_dir)
              end
            end
          end
        end
      end
    end
    create_hooks = [database.pre_create_dirs, database.post_create_dirs]
    import_hooks = database.imports.values.collect { |i| [i.pre_import_dirs, i.post_import_dirs] }
    database_wide_dirs = create_hooks + import_hooks
    database_wide_dirs.flatten.compact.each do |relative_dir_name|
      target_dir = "#{package_dir}/#{relative_dir_name}"
      actual_dirs = database.dirs_for_database(relative_dir_name)
      files = collect_files(actual_dirs)
      cp_files_to_dir(files, target_dir)
      generate_index(target_dir, files)
    end
    database.dirs_for_database('.').each do |dir|
      repository_file = "#{dir}/#{Dbt::Config.repository_config_file}"
      cp repository_file, package_dir if File.exist?(repository_file)
    end
    if database.enable_migrations?
      target_dir = "#{package_dir}/#{database.migrations_dir_name}"
      actual_dirs = database.dirs_for_database(database.migrations_dir_name)
      files = collect_files(actual_dirs)
      cp_files_to_dir(files, target_dir)
      generate_index(target_dir, files)
    end
  end

  def self.generate_index(target_dir, files)
    unless files.empty?
      File.open("#{target_dir}/#{Dbt::Config.index_file_name}", "w") do |index_file|
        index_file.write files.collect { |f| File.basename(f) }.join("\n")
      end
    end
  end

  def self.backup(database)
    init_control_database(database.key) do
      db.backup(database, configuration_for_database(database))
    end
  end

  def self.restore(database)
    init_control_database(database.key) do
      db.restore(database, configuration_for_database(database))
    end
  end

  def self.create_database(database)
    configuration = configuration_for_database(database)
    return if configuration.no_create?
    init_control_database(database.key) do
      db.drop(database, configuration)
      db.create_database(database, configuration)
    end
  end

  def self.database_import(imp, module_group)
    init_database(imp.database.key) do
      perform_import_action(imp, true, module_group)
    end
  end

  def self.up_module_group(module_group)
    database = module_group.database
    init_database(database.key) do
      database.modules.each do |module_name|
        next unless module_group.modules.include?(module_name)
        create_module(database, module_name, :up)
        create_module(database, module_name, :finalize)
      end
    end
  end

  def self.down_module_group(module_group)
    database = module_group.database
    init_database(database.key) do
      database.modules.reverse.each do |module_name|
        next unless module_group.modules.include?(module_name)
        process_module(database, module_name, :down)
        tables = database.table_ordering(module_name)
        schema_name = database.schema_name_for_module(module_name)
        db.drop_schema(schema_name, tables)
      end
    end
  end

  def self.create_by_import(imp)
    database = imp.database
    create_database(database) unless partial_import_completed?
    init_database(database.key) do
      perform_pre_create_hooks(database) unless partial_import_completed?
      perform_create_action(database, :up) unless partial_import_completed?
      perform_import_action(imp, false, nil)
      perform_create_action(database, :finalize)
      perform_post_create_hooks(database)
      perform_post_create_migrations_setup(database)
    end
  end

  def self.load_database_config(database)
    unless database.modules
      if database.load_from_classloader?
        content = load_resource(database, Dbt::Config.repository_config_file)
        parse_repository_config(database, content)
      else
        database.dirs_for_database('.').each do |dir|
          repository_config_file = "#{dir}/#{Dbt::Config.repository_config_file}"
          if File.exist?(repository_config_file)
            if database.modules
              raise "Duplicate copies of #{Dbt::Config.repository_config_file} found in database search path"
            else
              File.open(repository_config_file, 'r') do |f|
                parse_repository_config(database, f)
              end
            end
          end
        end
        raise "#{Dbt::Config.repository_config_file} not located in base directory of database search path and no modules defined" if database.modules.nil?
      end
    end
    database.validate
  end

  def self.parse_repository_config(database, content)
    require 'yaml'
    repository_config = YAML::load(content)
    database.modules = repository_config['modules'].collect { |m| m[0] }
    schema_overrides = {}
    table_map = {}
    repository_config['modules'].each do |module_config|
      name = module_config[0]
      schema = module_config[1]['schema']
      tables = module_config[1]['tables']
      table_map[name] = tables
      schema_overrides[name] = schema if name != schema
    end
    database.schema_overrides = schema_overrides
    database.table_map = table_map
  end

  def self.create(database)
    create_database(database)
    init_database(database.key) do
      perform_pre_create_hooks(database)
      perform_create_action(database, :up)
      perform_create_action(database, :finalize)
      perform_post_create_hooks(database)
      perform_post_create_migrations_setup(database)
    end
  end

  def self.migrate(database)
    init_database(database.key) do
      perform_migration(database, :perform)
    end
  end

  def self.perform_post_create_migrations_setup(database)
    if database.enable_migrations?
      db.setup_migrations
      if database.assume_migrations_applied_at_create?
        perform_migration(database, :record)
      else
        perform_migration(database)
      end
    end
  end

  def self.perform_migration(database, action)
    files =
      if database.load_from_classloader?
        collect_resources(database, database.migrations_dir_name)
      else
        collect_files(database.dirs_for_database(database.migrations_dir_name))
      end
    files.each do |filename|
      migration_name = File.basename(filename, '.sql')
      if db.should_migrate?(database.key.to_s, migration_name)
        run_sql_file(database, "Migration: ", filename, false) unless :record == action
        db.mark_migration_as_run(database.key.to_s, migration_name)
      end
    end
  end

  def self.load_datasets_for_modules( database, dataset_name )
    init_database(database.key) do
      database.modules.each do |module_name|
        load_dataset(database, module_name, dataset_name)
      end
    end
  end

  def self.perform_post_create_hooks(database)
    database.post_create_dirs.each do |dir|
      process_dir_set(database, dir, false, "#{'%-15s' % ''}: #{dir_display_name(dir)}")
    end
  end

  def self.perform_pre_create_hooks(database)
    database.pre_create_dirs.each do |dir|
      process_dir_set(database, dir, false, "#{'%-15s' % ''}: #{dir_display_name(dir)}")
    end
  end

  def self.drop(database)
    init_control_database(database.key) do
      db.drop(database, configuration_for_database(database))
    end
  end

  def self.import(imp, module_name, should_perform_delete)
    ordered_tables = imp.database.table_ordering(module_name)

    # check the import configuration is set
    configuration_for_key(config_key(imp.database.key, "import"))

    # Iterate over module in dependency order doing import as appropriate
    # Note: that tables with initial fixtures are skipped
    tables = ordered_tables.reject do |table|
      try_find_file_in_module(imp.database, module_name, Dbt::Config.fixture_dir_name, table, 'yml')
    end

    unless imp.database.load_from_classloader?
      dirs = imp.database.search_dirs.map { |d| "#{d}/#{module_name}/#{imp.dir}" }
      filesystem_files = dirs.collect { |d| Dir["#{d}/*.yml"] + Dir["#{d}/*.sql"] }.flatten.compact
      tables.each do |table_name|
        table_name = Dbt.clean_table_name(table_name)
        sql_file = /#{table_name}.sql$/
        yml_file = /#{table_name}.yml$/
        filesystem_files = filesystem_files.delete_if{|f| f =~ sql_file || f =~ yml_file}
      end
      raise "Discovered additional files in import directory in database search path. Files: #{filesystem_files.inspect}" unless filesystem_files.empty?
    end

    if should_perform_delete && !partial_import_completed?
      tables.reverse.each do |table|
        info("Deleting #{clean_table_name(table)}")
        run_sql_batch("DELETE FROM #{table}")
      end
    end

    tables.each do |table|
      if ENV[IMPORT_RESUME_AT_ENV_KEY] == Dbt.clean_table_name(table)
        run_sql_batch("DELETE FROM #{table}")
        ENV[IMPORT_RESUME_AT_ENV_KEY] = nil
      end
      unless partial_import_completed?
        db.pre_table_import(imp, table)
        perform_import(imp.database, module_name, table, imp.dir)
        db.post_table_import(imp, table)
      end
    end

    if ENV[IMPORT_RESUME_AT_ENV_KEY].nil?
      db.post_data_module_import(imp, module_name)
    end
  end

  def self.create_module(database, module_name, mode)
    schema_name = database.schema_name_for_module(module_name)
    db.create_schema(schema_name)
    process_module(database, module_name, mode)
  end

  def self.perform_create_action(database, mode)
    database.modules.each do |module_name|
      create_module(database, module_name, mode)
    end
  end

  def self.collect_resources(database, dir)
    index_name = cleanup_resource_name("#{dir}/#{Dbt::Config.index_file_name}")
    return [] unless resource_present?(database, index_name)
    load_resource(database, index_name).split("\n").collect{|l|cleanup_resource_name("#{dir}/#{l.strip}")}
  end

  def self.cleanup_resource_name(value)
    value.gsub(/\/\.\//,'/')
  end

  def self.collect_files(directories)

    index = []
    files = []

    directories.each do |dir|

      index_file = File.join(dir, Dbt::Config.index_file_name)
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

    unless duplicates.empty?
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
        process_dir_set(imp.database, dir, true, dir_display_name(dir))
      end unless partial_import_completed?
    end
    imp.modules.each do |module_key|
      if module_group.nil? || module_group.modules.include?(module_key)
        import(imp, module_key, should_perform_delete)
      end
    end
    if partial_import_completed?
      raise "Partial import unable to be completed as bad table name supplied #{ENV[IMPORT_RESUME_AT_ENV_KEY]}"
    end
    if module_group.nil?
      imp.post_import_dirs.each do |dir|
        process_dir_set(imp.database, dir, true, "#{'%-15s' % ''}: #{dir_display_name(dir)}")
      end
    end
    db.post_database_import(imp)
  end

  def self.process_dir_set(database, dir, is_import, label)
    files =
      if database.load_from_classloader?
        collect_resources(database, dir)
      else
        collect_files(database.dirs_for_database(dir))
      end
    run_sql_files(database, label, files, is_import)
  end

  def self.dir_display_name(dir)
    (dir == '.' ? '' : "#{dir}/")
  end

  def self.global_init
    @@database_driver_hooks.each do |database_hook|
      database_hook.call
    end

    require 'yaml'
    require 'erb'
    self.configuration_data = YAML::load(ERB.new(IO.read(Dbt::Config.config_filename)).result)
  end

  def self.config_key(database_key, env = Dbt::Config.environment)
    default_database?(database_key) ? env : "#{database_key}_#{env}"
  end

  def self.default_database?(database_key)
    database_key.to_s == Dbt::Config.default_database.to_s
  end

  def self.default_import?(import_key)
    import_key.to_s == Dbt::Config.default_import.to_s
  end

  def self.run_import_sql(database, table, sql, script_file_name = nil, print_dot = false)
    sql = filter_sql(sql, database.expanded_filters)
    sql = sql.gsub(/@@TABLE@@/, table) if table
    sql = filter_database_name(sql, /@@SOURCE@@/, config_key(database.key, "import"))
    sql = filter_database_name(sql, /@@TARGET@@/, config_key(database.key))
    run_sql_batch(sql, script_file_name, print_dot, true)
  end

  def self.generate_standard_import_sql(table)
    sql = "INSERT INTO @@TARGET@@.#{table}("
    columns = db.column_names_for_table(table)
    sql += columns.join(', ')
    sql += ")\n  SELECT "
    sql += columns.join(', ')
    sql += " FROM @@SOURCE@@.#{table}\n"
    sql
  end

  def self.perform_standard_import(database, table)
    run_import_sql(database, table, generate_standard_import_sql(table))
  end

  def self.perform_import(database, module_name, table, import_dir)
    fixture_file = try_find_file_in_module(database, module_name, import_dir, table, 'yml')
    sql_file = try_find_file_in_module(database, module_name, import_dir, table, 'sql')

    if fixture_file && sql_file
      raise "Unexpectantly found both import fixture (#{fixture_file}) and import sql (#{sql_file}) files."
    end

    info("#{'%-15s' % module_name}: Importing #{Dbt.clean_table_name(table)} (By #{fixture_file ? 'F' : sql_file ? 'S' : "D"})")
    if fixture_file
      load_fixture(table, load_data(database, fixture_file))
    elsif sql_file
      run_import_sql(database, table, load_data(database, sql_file), sql_file, true)
    else
      perform_standard_import(database, table)
    end
  end

  def self.setup_connection(database_key, open_control_database, &block)
    db.open(configuration_for_key(config_key(database_key)), open_control_database)
    if block_given?
      begin
        yield
      ensure
        db.close
      end
    end
  end

  def self.process_module(database, module_name, mode)
    dirs = mode == :up ? database.up_dirs : mode == :down ? database.down_dirs : database.finalize_dirs
    dirs.each do |dir|
      process_dir_set(database,"#{module_name}/#{dir}",false,"#{'%-15s' % module_name}: #{dir_display_name(dir)}")
    end
    load_fixtures(database, module_name) if mode == :up
  end

  def self.load_fixtures(database, module_name)
    load_fixtures_from_dirs(database, module_name, Dbt::Config.fixture_dir_name)
  end

  def self.load_dataset(database, module_name, dataset_name)
    load_fixtures_from_dirs(database, module_name, "#{Dbt::Config.datasets_dir_name}/#{dataset_name}")
  end

  def self.load_fixtures_from_dirs(database, module_name, subdir)
    fixtures = {}
    unless database.load_from_classloader?
      dirs = database.search_dirs.map { |d| "#{d}/#{module_name}#{ subdir ? "/#{subdir}" : ''}" }
      filesystem_files = dirs.collect{|d|Dir["#{d}/*.yml"]}.flatten.compact
    end
    database.table_ordering(module_name).each do |table_name|
      if database.load_from_classloader?
        filename = module_filename(module_name, subdir, table_name, 'yml')
        if resource_present?(database,filename)
          fixtures[table_name] = filename
        end
      else
        dirs.each do |dir|
          filename = table_name_to_fixture_filename(dir, table_name)
          filesystem_files.delete(filename)
          if File.exists?(filename)
            raise "Duplicate fixture for #{table_name} found in database search paths" if fixtures[table_name]
            fixtures[table_name] = filename
          end
        end
      end
    end

    if !database.load_from_classloader? && !filesystem_files.empty?
      raise "Unknown fixtures found in database search paths. Files: #{filesystem_files.inspect}"
    end

    database.table_ordering(module_name).reverse.each do |table_name|
      if fixtures[table_name]
        run_sql_batch("DELETE FROM #{table_name}")
      end
    end

    database.table_ordering(module_name).each do |table_name|
      filename = fixtures[table_name]
      next unless filename
      info("#{'%-15s' % 'Fixture'}: #{clean_table_name(table_name)}")
      load_fixture(table_name, load_data(database, filename))
    end
  end

  def self.table_name_to_fixture_filename(dir, table_name)
    "#{dir}/#{clean_table_name(table_name)}.yml"
  end

  def self.clean_table_name(table_name)
    table_name.tr('[]"''', '')
  end

  def self.load_fixture(table_name, content)
    yaml = YAML::load(ERB.new(content).result)
    # Skip empty files
    return unless yaml
    # NFI
    yaml_value =
      if yaml.respond_to?(:type_id) && yaml.respond_to?(:value)
        yaml.value
      else
        [yaml]
      end
    db.pre_fixture_import(table_name)
    yaml_value.each do |fixture|
      raise "Bad data for #{table_name} fixture named #{fixture}" unless fixture.respond_to?(:each)
      fixture.each do |name, data|
        raise "Bad data for #{table_name} fixture named #{name} (nil)" unless data
        db.insert(table_name, data)
      end
      db.post_fixture_import(table_name)
    end
  end

  def self.run_sql_batch(sql, script_file_name = nil, print_dot = false, execute_in_control_database = false)
    sql.gsub(/\r/, '').split(/(\s|^)GO(\s|$)/).reject { |q| q.strip.empty? }.each_with_index do |ddl, index|
      $stdout.putc '.' if print_dot
      begin
        db.execute(ddl, execute_in_control_database)
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

  def self.configuration_for_key(config_key)
    existing = @@configurations[config_key.to_s]
    return existing if existing
    c = self.configuration_data[config_key.to_s]
    raise "Missing config for #{config_key}" unless c
    configuration = Dbt.const_get("#{Dbt::Config.driver}DbConfig").new(c)
    @@configurations[config_key.to_s] = configuration
  end

  def self.configuration_data
    @@configuration_data
  end

  def self.configuration_data=(configuration_data)
    @@configuration_data = configuration_data
    @@configurations = {}
  end

  def self.run_filtered_sql_batch(database, sql, script_file_name = nil)
    sql = filter_sql(sql, database.expanded_filters)
    run_sql_batch(sql, script_file_name)
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

  def self.load_data(database, filename)
    if database.load_from_classloader?
      load_resource(database, filename)
    else
      IO.readlines(filename).join
    end
  end

  def self.run_sql_file(database, label, filename, is_import)
    info("#{label}#{File.basename(filename)}")
    sql = load_data(database, filename)
    if is_import
      run_import_sql(database, nil, sql, filename)
    else
      run_filtered_sql_batch(database, sql, filename)
    end
  end

  def self.resource_present?(database, resource_path)
    require 'java'
    !!java.lang.ClassLoader.getCallerClassLoader().getResource("#{database.resource_prefix}/#{resource_path}")
  end

  def self.load_resource(database, resource_path)
    require 'java'
    stream = java.lang.ClassLoader.getCallerClassLoader().getResourceAsStream("#{database.resource_prefix}/#{resource_path}")
    raise "Missing resource #{resource_path}" unless stream
    content = ""
    while stream.available() > 0
      content << stream.read()
    end
    content
  end

  def self.module_filename(module_name, subdir, table, extension)
    "#{module_name}/#{subdir}/#{clean_table_name(table)}.#{extension}"
  end

  def self.try_find_file_in_module(database, module_name, subdir, table, extension)
    filename = module_filename(module_name, subdir, table, extension)
    if database.load_from_classloader?
      resource_present?(database, filename) ? filename : nil
    else
      filename = module_filename(module_name, subdir, table, extension)
      database.search_dirs.map do |d|
        file = "#{d}/#{filename}"
        return file if File.exist?(file)
      end
      return nil
    end
  end

  def self.banner(message, database_key)
    info("**** #{message}: (Database: #{database_key}, Environment: #{Dbt::Config.environment}) ****")
  end

  def self.info(message)
    puts message
  end

  def self.db
    @db ||= Dbt.const_get("#{Dbt::Config.driver}DbDriver").new
  end

  class DbConfig
    def initialize(configuration)
      raise NotImplementedError
    end

    def catalog_name
      raise NotImplementedError
    end

    def no_create?
      raise NotImplementedError
    end
  end

  class DbDriver
    def insert(table_name, row)
      raise NotImplementedError
    end

    def execute(sql, execute_in_control_database = false)
      raise NotImplementedError
    end

    def query(sql)
      raise NotImplementedError
    end

    def create_schema(schema_name)
      raise NotImplementedError
    end

    def drop_schema(schema_name, tables)
      raise NotImplementedError
    end

    def column_names_for_table(table)
      raise NotImplementedError
    end

    def open(config, open_control_database)
      raise NotImplementedError
    end

    def close
      raise NotImplementedError
    end

    def create_database(database, configuration)
      raise NotImplementedError
    end

    def drop(database, configuration)
      raise NotImplementedError
    end

    def backup(database, configuration)
      raise NotImplementedError
    end

    def restore(database, configuration)
      raise NotImplementedError
    end

    def setup_migrations
      raise NotImplementedError
    end

    def should_migrate?(namespace, migration_name)
      raise NotImplementedError
    end

    def mark_migration_as_run(namespace, migration_name)
      raise NotImplementedError
    end

    def pre_table_import(imp, table)
      raise NotImplementedError
    end

    def post_table_import(imp, table)
      raise NotImplementedError
    end

    def pre_fixture_import(table)
      raise NotImplementedError
    end

    def post_fixture_import(table)
      raise NotImplementedError
    end

    def post_data_module_import(imp, module_name)
      raise NotImplementedError
    end
  end
end
