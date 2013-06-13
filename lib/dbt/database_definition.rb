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

  class DatabaseDefinition < BaseElement
    include FilterContainer

    def initialize(key, options, &block)
      @key = key
      options = options.dup
      imports_config = options.delete(:imports)
      module_groups_config = options.delete(:module_groups)

      @imports = {}
      imports_config.keys.each do |import_key|
        add_import(import_key, imports_config[import_key])
      end if imports_config
      @module_groups = {}
      module_groups_config.keys.each do |module_group_key|
        add_module_group(module_group_key, module_groups_config[module_group_key])
      end if module_groups_config

      @migrations = @backup = @modules = @restore = @datasets = @resource_prefix =
        @up_dirs = @down_dirs = @finalize_dirs = @pre_create_dirs = @post_create_dirs =
          @search_dirs = @migrations_dir_name = @migrations_applied_at_create =
            @rake_integration = @separate_import_task = @import_task_as_part_of_create =
              @schema_overrides = @datasets_dir_name = @fixture_dir_name =
                @database_environment_filter = @import_assert_filters = nil

      raise "schema_overrides should be derived from repository.yml and not directly specified." if options[:schema_overrides]
      raise "modules should be derived from repository.yml and not directly specified." if options[:modules]

      super(key, options, &block)
    end

    def add_import(import_key, import_config = {})
      @imports[import_key.to_s] = ImportDefinition.new(self, import_key, import_config)
    end

    def add_module_group(module_group_key, module_group_config)
      @module_groups[module_group_key.to_s] = ModuleGroupDefinition.new(self, module_group_key, module_group_config)
    end

    def validate
      @imports.values.each { |d| d.validate }
      @module_groups.values.each { |d| d.validate }
    end

    # symbolic name of database
    attr_reader :key

    # List of modules to import
    attr_reader :imports

    def import_by_name(import_key)
      import = @imports[import_key.to_s]
      raise "Unable to locate import definition by key '#{import_key}'" unless import
      import
    end

    # List of module_groups configs
    attr_reader :module_groups

    def module_group_by_name(module_group_key)
      module_group = @module_groups[module_group_key.to_s]
      raise "Unable to locate module group definition by key '#{module_group_key}'" unless module_group
      module_group
    end

    attr_writer :migrations

    def enable_migrations?
      @migrations.nil? ? false : !!@migrations
    end

    attr_writer :migrations_applied_at_create

    def assume_migrations_applied_at_create?
      @migrations_applied_at_create.nil? ? enable_migrations? : @migrations_applied_at_create
    end

    attr_writer :rake_integration

    def enable_rake_integration?
      @rake_integration.nil? ? true : @rake_integration
    end

    def task_prefix
      raise "task_prefix invoked" unless enable_rake_integration?
      "#{Dbt::Config.task_prefix}#{Dbt::Config.default_database?(self.key) ? '' : ":#{self.key}"}"
    end

    attr_writer :modules

    # List of modules to process for database
    def modules
      @modules = @modules.call if !@modules.nil? && @modules.is_a?(Proc)
      @modules
    end

    # Database version. Stuffed as an extended property and used when creating filename.
    attr_accessor :version

    attr_writer :datasets_dir_name

    def datasets_dir_name
      @datasets_dir_name || Dbt::Config.default_datasets_dir_name
    end

    attr_writer :fixture_dir_name

    def fixture_dir_name
      @fixture_dir_name || Dbt::Config.default_fixture_dir_name
    end

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

    attr_writer :separate_import_task

    def enable_separate_import_task?
      @separate_import_task.nil? ? false : @separate_import_task
    end

    attr_writer :import_task_as_part_of_create

    def enable_import_task_as_part_of_create?
      @import_task_as_part_of_create.nil? ? false : @import_task_as_part_of_create
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
      @schema_overrides ||= {}
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

    def parse_repository_config(content)
      require 'yaml'
      repository_config = YAML::load(content)
      self.modules = repository_config['modules'].collect { |m| m[0] }
      schema_overrides = {}
      table_map = {}
      repository_config['modules'].each do |module_config|
        name = module_config[0]
        schema = module_config[1]['schema']
        tables = module_config[1]['tables']
        table_map[name] = tables
        schema_overrides[name] = schema if name != schema
      end
      self.schema_overrides = schema_overrides
      self.table_map = table_map
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
end
