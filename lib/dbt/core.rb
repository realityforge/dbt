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

  @@defined_init_tasks = false
  @@database_driver_hooks = []
  @@repository = Repository.new
  @@runtime = Runtime.new

  def self.repository
    @@repository
  end

  def self.runtime
    @@runtime
  end

  def self.add_database_driver_hook(&block)
    @@database_driver_hooks << block
  end

  def self.database_for_key(database_key)
    @@repository.database_for_key(database_key)
  end

  def self.database_keys
    @@repository.database_keys
  end

  def self.add_database(database_key, options = {}, &block)
    database = @@repository.add_database(database_key, options, &block)

    define_tasks_for_database(database) if database.enable_rake_integration?

    database
  end

  def self.remove_database(database_key)
    @@repository.remove_database(database_key)
  end

  def self.define_database_package(database_key, buildr_project, options = {})
    database = @@repository.database_for_key(database_key)
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

    dependencies.each do |spec|
      jar.merge(Buildr.artifact(spec))
    end
    jar.include "#{package_dir}/code", :as => '.'
    jar.include "#{package_dir}/data"
    jar.with :manifest => buildr_project.manifest.merge('Main-Class' => 'org.realityforge.dbt.dbtcli')
  end


  private

  def self.define_tasks_for_database(database)
    self.define_basic_tasks
    task "#{database.task_prefix}:load_config" => ["#{Dbt::Config.task_prefix}:global:load_config"]

    # Database dropping

    desc "Drop the #{database.key} database."
    task "#{database.task_prefix}:drop" => ["#{database.task_prefix}:load_config"] do
      banner('Dropping database', database.key)
      @@runtime.drop(database)
    end

    # Database creation

    task "#{database.task_prefix}:pre_build" => ["#{Dbt::Config.task_prefix}:all:pre_build"]

    task "#{database.task_prefix}:prepare_fs" => ["#{database.task_prefix}:pre_build"] do
      @@runtime.load_database_config(database)
    end

    task "#{database.task_prefix}:prepare" => ["#{database.task_prefix}:load_config", "#{database.task_prefix}:prepare_fs"]

    desc "Create the #{database.key} database."
    task "#{database.task_prefix}:create" => ["#{database.task_prefix}:prepare"] do
      banner('Creating database', database.key)
      @@runtime.create(database)
    end

    # Data set loading etc
    database.datasets.each do |dataset_name|
      desc "Loads #{dataset_name} data"
      task "#{database.task_prefix}:datasets:#{dataset_name}" => ["#{database.task_prefix}:prepare"] do
        banner("Loading Dataset #{dataset_name}", database.key)
        @@runtime.load_dataset(database, dataset_name)
      end
    end

    if database.enable_migrations?
      desc "Apply migrations to bring data to latest version"
      task "#{database.task_prefix}:migrate" => ["#{database.task_prefix}:prepare"] do
        banner("Migrating", database.key)
        @@runtime.migrate(database)
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
        key = ":" + imp.key.to_s unless Dbt::Config.default_import?(imp.key)
        desc "Create the #{database.key} database by import."
        task "#{database.task_prefix}:create_by_import#{key}" => ["#{database.task_prefix}:prepare"] do
          banner("Creating Database By Import", database.key)
          @@runtime.create_by_import(imp)
        end
      end
    end

    if database.backup?
      desc "Perform backup of #{database.key} database"
      task "#{database.task_prefix}:backup" => ["#{database.task_prefix}:load_config"] do
        banner("Backing up Database", database.key)
        @@runtime.backup(database)
      end
    end

    if database.restore?
      desc "Perform restore of #{database.key} database"
      task "#{database.task_prefix}:restore" => ["#{database.task_prefix}:load_config"] do
        banner("Restoring Database", database.key)
        @@runtime.restore(database)
      end
    end
  end

  def self.define_module_group_tasks(module_group)
    database = module_group.database
    desc "Up the #{module_group.key} module group in the #{database.key} database."
    task "#{database.task_prefix}:#{module_group.key}:up" => ["#{database.task_prefix}:prepare"] do
      banner("Upping module group '#{module_group.key}'", database.key)
      @@runtime.up_module_group(module_group)
    end

    desc "Down the #{module_group.key} schema group in the #{database.key} database."
    task "#{database.task_prefix}:#{module_group.key}:down" => ["#{database.task_prefix}:prepare"] do
      banner("Downing module group '#{module_group.key}'", database.key)
      @@runtime.down_module_group(module_group)
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
    is_default_import = Dbt::Config.default_import?(imp.key)
    desc_prefix = is_default_import ? 'Import' : "#{imp.key.to_s.capitalize} import"

    task_name = is_default_import ? :import : :"import:#{imp.key}"
    desc "#{desc_prefix} #{description} of the #{imp.database.key} database."
    task "#{prefix}:#{task_name}" => ["#{imp.database.task_prefix}:prepare"] do
      banner("Importing Database#{is_default_import ? '' : " (#{imp.key})"}", imp.database.key)
      @@runtime.database_import(imp, module_group)
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
      @@runtime.create(database)
    elsif "drop" == command
      @@runtime.drop(database)
    elsif "migrate" == command
      @@runtime.migrate(database)
    elsif "restore" == command
      @@runtime.restore(database)
    elsif "backup" == command
      @@runtime.backup(database)
    elsif /^datasets:/ =~ command
      dataset_name = command[9, command.length]
      @@runtime.load_dataset(database, dataset_name)
    elsif /^import/ =~ command
      import_key = command[7, command.length]
      import_key = Dbt::Config.default_import.to_s if import_key.nil?
      database.imports.values.each do |imp|
        if imp.key.to_s == import_key
          @@runtime.database_import(imp, nil)
          return
        end
      end
      raise "Unknown import '#{import_key}'"
    elsif /^create_by_import/ =~ command
      import_key = command[17, command.length]
      import_key = Dbt::Config.default_import.to_s if import_key.nil?
      database.imports.values.each do |imp|
        if imp.key.to_s == import_key
          @@runtime.create_by_import(imp)
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
    @@runtime.package_database_data(database, "#{package_dir}/data")
  end

  def self.package_database_code(database, package_dir)
    FileUtils.mkdir_p package_dir
    valid_commands = ["create", "drop"]
    valid_commands << "restore" if database.restore?
    valid_commands << "backup" if database.backup?
    if database.enable_separate_import_task?
      database.imports.values.each do |imp|
        command = "import"
        command = "#{command}:#{imp.key}" unless Dbt::Config.default_import?(imp.key)
        valid_commands << command
      end
    end
    if database.enable_import_task_as_part_of_create?
      database.imports.values.each do |imp|
        command = "create_by_import"
        command = "#{command}:#{imp.key}" unless Dbt::Config.default_import?(imp.key)
        valid_commands << command
      end
    end
    database.datasets.each do |dataset|
      valid_commands << "datasets:#{dataset}"
    end

    valid_commands << "migrate" if database.enable_migrations?

    FileUtils.mkdir_p "#{package_dir}/org/realityforge/dbt"
    File.open("#{package_dir}/org/realityforge/dbt/dbtcli.rb", "w") do |f|
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
  database.fixture_dir_name = "#{database.fixture_dir_name}"
  database.datasets_dir_name = "#{database.datasets_dir_name}"
  database.migrations_dir_name = "#{database.migrations_dir_name}"
  database.up_dirs = %w(#{database.up_dirs.join(' ')})
  database.down_dirs = %w(#{database.down_dirs.join(' ')})
  database.finalize_dirs = %w(#{database.finalize_dirs.join(' ')})
  database.pre_create_dirs = %w(#{database.pre_create_dirs.join(' ')})
  database.post_create_dirs = %w(#{database.post_create_dirs.join(' ')})
  database.datasets = %w(#{database.datasets.join(' ')})
  database.import_assert_filters = #{database.import_assert_filters?}
  database.database_environment_filter = #{database.database_environment_filter?}
TXT

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
Dbt.runtime.load_database_config(database)

ARGV.each do |command|
  Dbt.execute_command(database, command)
end
TXT
    end
    sh "jrubyc --dir #{::Buildr::Util.relative_path(package_dir, Dir.pwd)} #{::Buildr::Util.relative_path(package_dir, Dir.pwd)}/org/realityforge/dbt/dbtcli.rb"
    FileUtils.cp_r Dir.glob("#{File.expand_path(File.dirname(__FILE__) + '/..')}/*"), package_dir
  end

  def self.global_init
    @@database_driver_hooks.each do |database_hook|
      database_hook.call
    end

    @@repository.load_configuration_data(Dbt::Config.config_filename)
  end

  def self.configuration_for_key(config_key)
    @@repository.configuration_for_key(config_key)
  end

  def self.banner(message, database_key)
    @@runtime.info("**** #{message}: (Database: #{database_key}, Environment: #{Dbt::Config.environment}) ****")
  end
end
