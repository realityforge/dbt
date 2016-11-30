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

class Dbt #nodoc

  def self.jruby_version(options)
    options[:jruby_version] || (defined?(JRUBY_VERSION) ? JRUBY_VERSION : '1.7.2')
  end

  def self.jruby_complete_jar(options)
    "org.jruby:jruby-complete:jar:#{jruby_version(options)}"
  end

  def self.define_database_package(database_key, options = {})
    buildr_project = options[:buildr_project]
    if buildr_project.nil? && ::Buildr.application.current_scope.size > 0
      buildr_project = ::Buildr.project(::Buildr.application.current_scope.join(':')) rescue nil
    end
    raise "Unable to determine Buildr project when generating #{database_key} database package" unless buildr_project
    database = self.repository.database_for_key(database_key)
    package_dir = buildr_project._(:target, 'dbt')
    include_code = options[:include_code].nil? || options[:include_code]

    task "#{database.task_prefix}:package" => ["#{database.task_prefix}:prepare_fs"] do
      banner('Packaging Database Scripts', database.key)
      params = options.dup
      params[:jruby_version] = jruby_version(options)
      params[:include_code] = include_code
      package_database(database, package_dir, params)
    end
    jar = buildr_project.package(:jar) do |j|
    end
    sources = buildr_project.package(:jar, :classifier => 'sources') do |j|
    end
    jar.include buildr_project.file("#{package_dir}/data" => "#{database.task_prefix}:package")
    sources.include buildr_project.file("#{package_dir}/data" => "#{database.task_prefix}:package")
    if include_code
      buildr_project.file("#{package_dir}/code" => "#{database.task_prefix}:package")
      dependencies =
        [jruby_complete_jar(options)] +
          Dbt::Config.driver_config_class(:jruby => true).jdbc_driver_dependencies

      dependencies.each do |spec|
        jar.merge(::Buildr.artifact(spec))
      end
      jar.include "#{package_dir}/code", :as => '.'
      jar.with :manifest => buildr_project.manifest.merge('Main-Class' => 'org.realityforge.dbt.dbtcli')
    end
  end

  private

  def self.execute_command(database, command)
    if 'status' == command
      puts @@runtime.status(database)
    elsif 'create' == command
      @@runtime.create(database)
    elsif 'drop' == command
      @@runtime.drop(database)
    elsif 'migrate' == command
      @@runtime.migrate(database)
    elsif 'restore' == command
      @@runtime.restore(database)
    elsif 'backup' == command
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
    elsif /^up/ =~ command
      module_group_key = command[3, command.length]
      module_group = database.module_group_by_name(module_group_key)
      @@runtime.up_module_group(module_group)
    elsif /^down/ =~ command
      module_group_key = command[5, command.length]
      module_group = database.module_group_by_name(module_group_key)
      @@runtime.down_module_group(module_group)
    else
      raise "Unknown command '#{command}'"
    end
  end

  def self.package_database(database, package_dir, options)
    rm_rf package_dir
    package_database_code(database, package_dir, options) if options[:include_code]
    self.runtime.package_database_data(database, "#{package_dir}/data")
  end

  def self.package_database_code(database, base_package_dir, options)
    package_dir = "#{base_package_dir}/code"
    FileUtils.mkdir_p package_dir
    valid_commands = %w(status create drop)
    valid_commands << 'restore' if database.restore?
    valid_commands << 'backup' if database.backup?
    if database.enable_separate_import_task?
      database.imports.values.each do |imp|
        command = 'import'
        command = "#{command}:#{imp.key}" unless Dbt::Config.default_import?(imp.key)
        valid_commands << command
      end
    end
    if database.enable_import_task_as_part_of_create?
      database.imports.values.each do |imp|
        command = 'create_by_import'
        command = "#{command}:#{imp.key}" unless Dbt::Config.default_import?(imp.key)
        valid_commands << command
      end
    end
    database.datasets.each do |dataset|
      valid_commands << "datasets:#{dataset}"
    end

    database.module_groups.keys.each do |key|
      valid_commands << "up:#{key}"
      valid_commands << "down:#{key}"
    end

    valid_commands << 'migrate' if database.enable_migrations?

    FileUtils.mkdir_p "#{package_dir}/org/realityforge/dbt"
    File.open("#{package_dir}/org/realityforge/dbt/dbtcli.rb", 'w') do |f|
      f << <<TXT
require 'dbt'
require 'optparse'
require 'java'

Dbt::Config.driver = '#{Dbt::Config.driver}'
Dbt::Config.environment = 'production'
Dbt::Config.config_filename = 'config/database.yml'
VALID_COMMANDS=#{valid_commands.inspect}

database_key = :default

opt_parser = OptionParser.new do |opt|
  opt.banner = 'Usage: dbtcli [OPTIONS] [COMMANDS]'
  opt.separator  ''
  opt.separator  "Commands: #{valid_commands.join(', ')}"
  opt.separator  ''
  opt.separator  'Options'

  opt.on('-d','--database KEY', "the database key to use. Defaults to 'default'.") do |key|
    database_key = key.to_sym
  end

  opt.on('-e','--environment ENV', "the database environment to use. Defaults to 'production'.") do |environment|
    Dbt::Config.environment = environment
  end

  opt.on('-c','--config-file CONFIG', "the configuration file to use. Defaults to 'config/database.yml'.") do |config_filename|
    Dbt::Config.config_filename = config_filename
  end

  opt.on('-h','--help','help') do
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
  puts 'No command specified'
  puts opt_parser
  java.lang.System.exit(31)
end

database = Dbt.add_database(database_key) do |database|
  database.resource_prefix = 'data'
  database.fixture_dir_name = '#{database.fixture_dir_name}'
  database.datasets_dir_name = '#{database.datasets_dir_name}'
  database.migrations_dir_name = '#{database.migrations_dir_name}'
  database.up_dirs = %w(#{database.up_dirs.join(' ')})
  database.down_dirs = %w(#{database.down_dirs.join(' ')})
  database.finalize_dirs = %w(#{database.finalize_dirs.join(' ')})
  database.pre_create_dirs = %w(#{database.pre_create_dirs.join(' ')})
  database.post_create_dirs = %w(#{database.post_create_dirs.join(' ')})
  database.datasets = %w(#{database.datasets.join(' ')})
  database.import_assert_filters = #{database.import_assert_filters?}
  database.database_environment_filter = #{database.database_environment_filter?}
  database.rake_integration = false
  database.migrations = #{database.enable_migrations?}
  database.version = #{database.version.inspect}
  database.version_hash = #{database.version_hash.inspect}
TXT
      database.module_groups.each_pair do |key, module_group|
        f << "   database.add_module_group(:#{key.to_s}, :modules => #{module_group.modules.inspect}, :import_enabled => #{module_group.import_enabled?})\n"
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
          :pre_import_dirs => definition.pre_import_dirs,
          :post_import_dirs => definition.post_import_dirs
        }
        f << "  database.add_import(:#{import_key}, #{import_config.inspect})\n"
      end

      f << <<TXT
end

puts "Environment: \#{Dbt::Config.environment}"
puts "Config File: \#{Dbt::Config.config_filename}"
puts "Commands: \#{ARGV.join(' ')}"

if Dbt.repository.load_configuration_data
  Dbt.runtime.load_database_config(database)

  ARGV.each do |command|
    Dbt.execute_command(database, command)
  end
else
  puts 'Unable to load database configuration'
  java.lang.System.exit(37)
end
TXT
    end
    FileUtils.cp_r Dir.glob("#{File.expand_path(File.dirname(__FILE__) + '/..')}/*"), package_dir

    spec = Gem::Specification::load(File.expand_path(File.dirname(__FILE__) + '/../../dbt.gemspec'))
    spec.dependencies.select { |dependency| dependency.type == :runtime }.each do |dep|
      dep_spec = Gem.loaded_specs[dep.name]
      dep_spec.require_paths.each do |path|
        lib_path = dep_spec.gem_dir + '/' + path + '/.'
        FileUtils.cp_r lib_path, package_dir
      end
    end

    jar = ::Buildr.artifact(jruby_complete_jar(options))
    dir = ::Buildr::Util.relative_path(package_dir, Dir.pwd)
    script = "require 'jruby/jrubyc';exit(JRuby::Compiler::compile_argv(ARGV))"
    java = Java::Commands.send(:path_to_bin, 'java')
    command = "#{java} -jar #{jar} --disable-gems -e \"#{script}\" -- --dir #{dir} #{Dir["#{dir}/**/*.rb"].join(' ')}"
    old_gemfile = ENV['BUNDLE_GEMFILE']
    ENV['BUNDLE_GEMFILE'] = "#{base_package_dir}/Gemfile"
    FileUtils.touch "#{base_package_dir}/Gemfile"
    begin
      sh command
    ensure
      ENV['BUNDLE_GEMFILE'] = old_gemfile
    end
  end
end
