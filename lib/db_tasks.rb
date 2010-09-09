# Note the following terminology is used throughout the plugin
# * database_key: a symbolic name of database. i.e. "central", "master", "core",
#   "ifis", "msdb" etc
# * env: a development environment. i.e. "test", "development", "production"
# * schema: name of the database directory in which sets of related database
#   files are stored. i.e. "audit", "auth", "interpretation", ...
# * config_key: the name of entry in YAML file to look up configuration. Typically
#   constructed by database_key and env separated by an underscore. i.e.
#   "central_development", "master_test" etc.

# It should also be noted that the in some cases there is a database_key and
# schema with the same name. This was due to legacy reasons and should be avoided
# in the future as it is confusing

class DbTasks

  class Config

    class << self
      attr_accessor :app_version

      attr_writer :environment

      def environment
        return 'development' unless @environment
        @environment
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
      attr_writer :search_dirs

      def search_dirs
        raise "search_dirs not specified" unless @search_dirs
        @search_dirs
      end

      attr_writer :sql_dirs

      def sql_dirs
        return ['.', 'types', 'views', 'functions', 'stored-procedures', 'triggers', 'misc'] unless @sql_dirs
        @sql_dirs
      end

      attr_writer :down_dirs

      # Return the list of dirs to prcess when downing module
      def down_dirs
        return ['down'] unless @down_dirs
        @down_dirs
      end
    end
  end

  @@filters = []
  @@table_order_resolver = nil
  @@defined_init_tasks = false
  @@database_driver_hooks = []

  def self.init(database_key, env)
    setup_connection(config_key(database_key, env))
  end

  def self.add_filter(&block)
    @@filters << block
  end

  def self.add_database_driver_hook(&block)
    @@database_driver_hooks << block
  end

  def self.add_database_name_filter(pattern, database_key)
    add_filter do |current_config, env, sql|
      filter_database_name(sql, pattern, current_config, "#{database_key}_#{env}", false)
    end
  end

  def self.define_table_order_resolver(&block)
    @@table_order_resolver = block
  end

  def self.add_database(database_key, modules, options = {})
    self.define_basic_tasks

    schema_2_module = {}
    modules.each do |module_name|
      schema_name = (options[:schema_overrides] ? options[:schema_overrides][module_name] : nil) || module_name
      (schema_2_module[schema_name] ||= []) << module_name
    end

    namespace :dbt do
      namespace database_key do
        desc "Create the #{database_key} database."
        task :create => ['dbt:load_config', "dbt:#{database_key}:banner", "dbt:#{database_key}:pre_build", "dbt:#{database_key}:build", "dbt:#{database_key}:post_build"]

        task "dbt:#{database_key}:banner" do
          DbTasks.info("**** Creating database: #{database_key} (Environment: #{DbTasks::Config.environment}) ****")
        end

        task :pre_build => ['dbt:load_config', 'dbt:pre_build']

        modules.each do |module_name|
          task :build => "post_module_#{module_name}"

          task "post_module_#{module_name}" => "dbt:#{database_key}:build_module_#{module_name}"

          task "build_module_#{module_name}" => "dbt:#{database_key}:pre_module_#{module_name}"

          task "pre_module_#{module_name}"
        end

        modules.each_with_index do |module_name, idx|
          task "build_module_#{module_name}" do
            recreate_db = idx == 0
            schema_name = (options[:schema_overrides] ? options[:schema_overrides][module_name] : nil) || module_name
            create_schema = schema_2_module[schema_name][0] == module_name
            DbTasks.create_module(database_key,
                                  DbTasks::Config.environment,
                                  module_name,
                                  recreate_db,
                                  schema_name,
                                  create_schema)
          end
        end

        task :post_build do
        end

        namespace :datasets do
          (options[:datasets] || []).each do |dataset_name|
            desc "Loads #{dataset_name} data"
            task dataset_name => ['dbt:load_config'] do
              modules.each do |module_name|
                DbTasks.load_dataset(database_key, DbTasks::Config.environment, module_name, dataset_name)
              end
            end
          end
        end

        (options[:schema_groups] || {}).each_pair do |schema_group_name, schemas|
          namespace schema_group_name do
            desc "Up the #{schema_group_name} schema group in the #{database_key} database."
            task :up => ['dbt:load_config', "dbt:#{database_key}:pre_build"]  do
              DbTasks.info("**** Upping schema group: #{schema_group_name} (Database: #{database_key}, Environment: #{DbTasks::Config.environment}) ****")
              modules.each do |module_name|
                schema_name = (options[:schema_overrides] ? options[:schema_overrides][module_name] : nil) || module_name
                next unless schemas.include?(schema_name)
                create_schema = schema_2_module[schema_name][0] == module_name
                DbTasks.create_module(database_key,
                                      DbTasks::Config.environment,
                                      module_name,
                                      false,
                                      schema_name,
                                      create_schema)
              end
            end

            desc "Down the #{schema_group_name} schema group in the #{database_key} database."
            task :down => ['dbt:load_config', "dbt:#{database_key}:pre_build"] do
              DbTasks.info("**** Downing schema group: #{schema_group_name} (Database: #{database_key}, Environment: #{DbTasks::Config.environment}) ****")
              DbTasks.init(database_key, DbTasks::Config.environment)
              modules.reverse.each do |module_name|
                schema_name = (options[:schema_overrides] ? options[:schema_overrides][module_name] : nil) || module_name
                next unless schemas.include?(schema_name)
                process_module(database_key, DbTasks::Config.environment, module_name, false)
              end
              schemas.reverse.each do |schema_name|
                DbTasks.drop_schema(schema_name, schema_2_module[schema_name])
              end
            end

            import_modules = (options[:import] || modules).select do |module_name|
              schema_name = (options[:schema_overrides] ? options[:schema_overrides][module_name] : nil) || module_name
              schemas.include?(schema_name)
            end

            if !import_modules.empty?
              desc "Import contents of the #{schema_group_name} schema group in the #{database_key} database."
              task :import => ['dbt:load_config'] do
                import_modules.each do |module_name|
                  DbTasks.import(database_key, DbTasks::Config.environment, module_name)
                end
              end
            end
          end
        end

        desc "Import contents of the #{database_key} database."
        task :import => ['dbt:load_config'] do
          import_modules = options[:import] || modules
          import_modules.each do |module_name|
            DbTasks.import(database_key, DbTasks::Config.environment, module_name)
          end
        end

        desc "Drop the #{database_key} database."
        task :drop => ['dbt:load_config'] do
          DbTasks.info("**** Dropping database: #{database_key} ****")
          DbTasks.drop(database_key, DbTasks::Config.environment)
        end
      end
    end
  end

  def self.create_module(database_key, env, module_name, create_database, schema_name, create_schema)
    key = config_key(database_key, env)
    physical_name = get_config(key)['database']
    create_database = false if true == get_config(key)['no_create']
    if create_database
      setup_connection("msdb")
      recreate_db(database_key, env, true)
    else
      setup_connection(key)
    end
    DbTasks.trace("Database Load [#{physical_name}]: module=#{module_name}, db=#{database_key}, env=#{env}, key=#{key}\n")
    if create_schema && schema_name.to_s != DEFAULT_SCHEMA.to_s
      run_filtered_sql(database_key, env, "CREATE SCHEMA [#{schema_name}]")
    end
    process_module(database_key, env, module_name, true)
  end

  def self.run_sql_in_dir(database_key, env, label, dir)
    check_dir(label, dir)
    Dir["#{dir}/*.sql"].sort.each do |sp|
      DbTasks.info("#{label}: #{File.basename(sp)}\n")
      run_filtered_sql(database_key, env, IO.readlines(sp).join)
    end
  end

  def self.import(database_key, env, module_name)
    ordered_tables = table_ordering(module_name)

    # check the database configurations are set
    target_config = config_key(database_key, env)
    source_config = config_key(database_key, "import")
    get_config(target_config)
    get_config(source_config)

    phsyical_name = get_config(target_config)['database']
    DbTasks.trace("Database Import [#{phsyical_name}]: module=#{module_name}, db=#{database_key}, env=#{env}, source_key=#{source_config} target_key=#{target_config}\n")
    setup_connection(target_config)

    # Iterate over module in dependency order doing import as appropriate
    # Note: that tables with initial fixtures are skipped
    tables = ordered_tables.reject do |table|
      fixture_for_creation(module_name, table)
    end
    tables.reverse.each do |table|
      DbTasks.info("Deleting #{table}\n")
      q_table = to_qualified_table_name(table)
      run_import_sql(database_key, env, "DELETE FROM @@TARGET@@.#{q_table}")
    end

    tables.each do |table|
      perform_import(database_key, env, module_name, table)
    end

    tables.each do |table|
      DbTasks.info("Reindexing #{table}\n")
      q_table = to_qualified_table_name(table)
      run_import_sql(database_key, env, "DBCC DBREINDEX (N'@@TARGET@@.#{q_table}', '', 0) WITH NO_INFOMSGS")
    end

    run_import_sql(database_key, env, "DBCC SHRINKDATABASE(N'@@TARGET@@', 10, NOTRUNCATE) WITH NO_INFOMSGS")
    run_import_sql(database_key, env, "DBCC SHRINKDATABASE(N'@@TARGET@@', 10, TRUNCATEONLY) WITH NO_INFOMSGS")
    run_import_sql(database_key, env, "EXEC @@TARGET@@.#{DEFAULT_SCHEMA}.sp_updatestats")
  end

  def self.drop(database_key, env)
    key = config_key(database_key, env)
    setup_connection("msdb")
    db = get_config(key)['database']
    force_drop = true == get_config(key)['force_drop']

    sql = if force_drop
      <<SQL
USE [msdb]
GO
  IF EXISTS
    ( SELECT *
      FROM  sys.master_files
      WHERE state = 0 AND db_name(database_id) = '#{db}')
    ALTER DATABASE [#{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
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
      WHERE state = 0 AND db_name(database_id) = '#{db}')
    DROP DATABASE [#{db}]
GO
SQL
    DbTasks.trace("Database Drop [#{db}]: database_key=#{database_key}, env=#{env}, key=#{key}\n")
    run_filtered_sql(database_key, env, sql)
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

  def self.load_tables_from_fixtures(tables, dir)
    raise "Fixture directory #{dir} does not exist" unless File.exists?(dir)
    ActiveRecord::Base.connection.transaction do
      Fixtures.create_fixtures(dir, tables)
    end
  end

  private

  DEFAULT_SCHEMA = 'dbo'

  def self.define_basic_tasks
    if !@@defined_init_tasks
      task 'dbt:load_config' do
        require 'activerecord'
        require 'active_record/fixtures'
        @@database_driver_hooks.each do |database_hook|
          database_hook.call
        end
        ActiveRecord::Base.configurations = YAML::load(ERB.new(IO.read(DbTasks::Config.config_filename)).result)
      end

      task 'dbt:pre_build' => ['dbt:load_config']

      @@defined_init_tasks = true
    end
  end

  def self.table_ordering(module_key)
    raise "No table resolver so unable to determine table ordering for module #{module_key}" unless @@table_order_resolver
    @@table_order_resolver.call(module_key)
  end

  def self.config_key(database_key, env)
    database_key.to_s == DbTasks::Config.default_database.to_s ? env : "#{database_key}_#{env}"
  end

  def self.to_qualified_table_name(table)
    elements = table.to_s.split('.')
    elements = [DEFAULT_SCHEMA, elements[0]] if elements.size == 1
    elements.join('.')
  end

  def self.run_import_sql(database_key, env, sql, change_to_msdb = true)
    target_config = config_key(database_key, env)
    source_config = config_key(database_key, "import")
    sql = filter_sql("msdb", "import", sql)
    sql = filter_database_name(sql, /@@SOURCE@@/, "msdb", source_config)
    sql = filter_database_name(sql, /@@TARGET@@/, "msdb", target_config)
    c = ActiveRecord::Base.connection
    current_database = get_config(target_config)["database"]
    if change_to_msdb
      c.execute "USE [msdb]"
      run_sql(sql)
      c.execute "USE [#{current_database}]"
    else
      run_sql(sql)
    end
  end

  def self.perform_standard_import(database_key, env, table)
    q_table = to_qualified_table_name(table)
    sql = "INSERT INTO @@TARGET@@.#{q_table}("
    columns = ActiveRecord::Base.connection.columns(q_table).collect { |c| "[#{c.name}]" }
    sql += columns.join(', ')
    sql += ")\n  SELECT "
    sql += columns.collect { |c| c == '[BatchID]' ? "0" : c }.join(', ')
    sql += " FROM @@SOURCE@@.#{q_table}\n"

    run_import_sql(database_key, env, sql)
  end

  def self.perform_import(database_key, env, module_name, table)
    has_identity = has_identity_column(table)

    q_table = to_qualified_table_name(table)

    run_import_sql(database_key, env, "SET IDENTITY_INSERT @@TARGET@@.#{q_table} ON") if has_identity
    run_import_sql(database_key, env, "EXEC sp_executesql \"DISABLE TRIGGER ALL ON @@TARGET@@.#{q_table}\"", false)

    fixture_file = fixture_for_import(module_name, table)
    sql_file = sql_for_import(module_name, table)
    is_sql = !fixture_file && sql_file

    DbTasks.info("Importing #{table} (By #{fixture_file ? 'F' : is_sql ? 'S' : "D"})\n")
    if fixture_file
      Fixtures.create_fixtures(File.dirname(fixture_file), table)
    elsif is_sql
      run_import_sql(database_key, env, IO.readlines(sql_file).join)
    else
      perform_standard_import(database_key, env, table)
    end

    run_import_sql(database_key, env, "EXEC sp_executesql \"ENABLE TRIGGER ALL ON @@TARGET@@.#{q_table}\"", false)
    run_import_sql(database_key, env, "SET IDENTITY_INSERT @@TARGET@@.#{q_table} OFF") if has_identity
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

  def self.recreate_db(database_key, env, cs = true)
    drop(database_key, env)
    key = config_key(database_key, env)
    config = get_config(key)
    db_name = config['database']
    collation = cs ? 'COLLATE SQL_Latin1_General_CP1_CS_AS' : ''
    if DbTasks::Config.app_version.nil?
      db_filename = db_name
    else
      db_filename = "#{db_name}_#{DbTasks::Config.app_version.gsub(/\./, '_')}"
    end
    db_def = config["data_path"] ? "ON PRIMARY (NAME = [#{db_filename}], FILENAME='#{config["data_path"]}#{"\\"}#{db_filename}.mdf')" : ""
    log_def = config["log_path"] ? "LOG ON (NAME = [#{db_filename}_LOG], FILENAME='#{config["log_path"]}#{"\\"}#{db_filename}.ldf')" : ""

    sql = <<SQL
CREATE DATABASE [#{db_name}] #{db_def} #{log_def} #{collation}
GO
ALTER DATABASE [#{db_name}] SET CURSOR_DEFAULT LOCAL
ALTER DATABASE [#{db_name}] SET CURSOR_CLOSE_ON_COMMIT ON

ALTER DATABASE [#{db_name}] SET AUTO_CREATE_STATISTICS ON
ALTER DATABASE [#{db_name}] SET AUTO_UPDATE_STATISTICS ON
ALTER DATABASE [#{db_name}] SET AUTO_UPDATE_STATISTICS_ASYNC ON

ALTER DATABASE [#{db_name}] SET ANSI_NULL_DEFAULT ON
ALTER DATABASE [#{db_name}] SET ANSI_NULLS ON
ALTER DATABASE [#{db_name}] SET ANSI_PADDING ON
ALTER DATABASE [#{db_name}] SET ANSI_WARNINGS ON
ALTER DATABASE [#{db_name}] SET ARITHABORT ON
ALTER DATABASE [#{db_name}] SET CONCAT_NULL_YIELDS_NULL ON
ALTER DATABASE [#{db_name}] SET QUOTED_IDENTIFIER ON
ALTER DATABASE [#{db_name}] SET NUMERIC_ROUNDABORT ON
ALTER DATABASE [#{db_name}] SET RECURSIVE_TRIGGERS ON

ALTER DATABASE [#{db_name}] SET RECOVERY SIMPLE

GO
  USE [#{db_name}]
SQL
    DbTasks.trace("Database Create [#{db_name}]: database=#{database_key}, env=#{env}, config=#{key}\n")
    run_filtered_sql(database_key, env, sql)
  end

  def self.process_module(database_key, env, module_name, is_build)
    dirs = is_build ? DbTasks::Config.sql_dirs : DbTasks::Config.down_dirs
    dirs.each do |dir|
      run_sql_in_dirs(database_key, env, (dir == '.' ? 'Base' : dir.humanize), dirs_for_module(module_name, dir))
    end
    if is_build
      load_fixtures_from_dirs(module_name, dirs_for_module(module_name, 'fixtures'))
    end
  end

  def self.check_dir(name, dir)
    raise "#{name} in missing dir #{dir}" unless File.exists?(dir)
  end

  def self.load_dataset(database_key, env, module_name, dataset_name)
    setup_connection(config_key(database_key, env))
    load_fixtures_from_dirs(module_name, dirs_for_module(module_name, "datasets/#{dataset_name}"))
  end

  def self.load_fixtures_from_dirs(module_name, dirs)
    require 'active_record/fixtures'
    dir = dirs.select { |dir| File.exists?(dir) }[0]
    return unless dir
    files = []
    table_ordering(module_name).each do |t|
      files += [t] if File.exist?("#{dir}/#{t}.yml")
    end
    DbTasks.info("Loading fixtures: #{files.join(',')}")
    Fixtures.create_fixtures(dir, files)
  end

  def self.run_sql(sql)
    sql.gsub(/\r/, '').split("\nGO\n").each do |ddl|
      # Transaction required to work around a bug that sometimes leaves last
      # SQL command before shutting the connection un committed.
      ActiveRecord::Base.connection.transaction do
        ActiveRecord::Base.connection.execute(ddl, nil)
      end
    end
  end

  def self.drop_schema(schema_name, modules)
    database_objects("SQL_STORED_PROCEDURE", schema_name).each { |name| run_sql("DROP PROCEDURE #{name}") }
    database_objects("SQL_SCALAR_FUNCTION", schema_name).each { |name| run_sql("DROP FUNCTION #{name}") }
    database_objects("SQL_INLINE_TABLE_VALUED_FUNCTION", schema_name).each { |name| run_sql("DROP FUNCTION #{name}") }
    database_objects("SQL_TABLE_VALUED_FUNCTION", schema_name).each { |name| run_sql("DROP FUNCTION #{name}") }
    database_objects("VIEW", schema_name).each { |name| run_sql("DROP VIEW #{name}") }
    modules.reverse.each do |module_name|
      table_ordering(module_name).reverse.each do |t|
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

  def self.run_filtered_sql(database_key, env, sql)
    sql = filter_sql(config_key(database_key, env), env, sql)
    run_sql(sql)
  end

  def self.filter_sql(config_key, env, sql, filters = @@filters)
    filters.each do |filter|
      sql = filter.call(config_key, env, sql)
    end
    sql = filter_database_name(sql, /@@SELF@@/, config_key, config_key)
    sql
  end

  def self.run_sql_in_dirs(database_key, env, label, dirs)
    dirs.each do |dir|
      run_sql_in_dir(database_key, env, label, dir) if File.exists?(dir)
    end
  end

  def self.run_sql_in_dir(database_key, env, label, dir)
    check_dir(label, dir)
    Dir["#{dir}/*.sql"].sort.each do |sp|
      DbTasks.info("#{label}: #{File.basename(sp)}\n")
      run_filtered_sql(database_key, env, IO.readlines(sp).join)
    end
  end

  def self.dirs_for_module(module_name, subdir = nil)
    DbTasks::Config.search_dirs.map { |d| "#{d}/#{module_name}#{ subdir ? "/#{subdir}" : ''}" }
  end

  def self.first_file_from(files)
    files.each do |file|
      if File.exist?(file)
        return file
      end
    end
    nil
  end

  def self.fixture_for_creation(module_name, table)
    first_file_from(dirs_for_module(module_name, "fixtures/#{table}.yml"))
  end

  def self.fixture_for_import(module_name, table)
    first_file_from(dirs_for_module(module_name, "import/#{table}.yml"))
  end

  def self.sql_for_import(module_name, table)
    first_file_from(dirs_for_module(module_name, "import/#{table}.sql"))
  end

  def self.info(message)
    puts message
  end

  def self.trace(message)
    puts message if ActiveRecord::Migration.verbose
  end
end
