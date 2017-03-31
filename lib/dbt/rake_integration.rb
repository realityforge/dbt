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

  @@defined_init_tasks = false

  class DatabaseDefinition #nodoc

    # Enable domgen support. Assume the database is associated with a single repository
    # definition, a single task to generate sql etc. By convention the loading task name
    # is 'domgen:load' and the generate task is 'domgen:sql' and these will be used if no
    # other values are specified. The repository_key can be derived if there is only one
    # repository in the system, otherwise it must be specified.
    def enable_domgen(repository_key = nil, load_task_name = nil, generate_task_name = nil)
      task "#{task_prefix}:load_config" => (load_task_name || 'domgen:load')
      task "#{task_prefix}:pre_build" => (generate_task_name || 'domgen:sql')

      desc 'Verify constraints on database.'
      task "#{task_prefix}:verify_constraints" => ["#{task_prefix}:load_config"] do
        Dbt.banner('Verifying database', key)
        repository = get_domgen_repository(repository_key)

        failed_constraints = []
        repository.data_modules.select { |data_module| data_module.sql? }.each do |data_module|
          failed_constraints += Dbt.runtime.query(self, "EXEC #{data_module.sql.schema}.spCheckConstraints")
        end
        if failed_constraints.size > 0
          error_message = "Failed Constraints:\n#{failed_constraints.collect do |row|
            "\t#{row['ConstraintName']} on #{row['SchemaName']}.#{row['TableName']}"
          end.join("\n")}"
          raise error_message
        end
        Dbt.banner('Database verified', key)
      end

      desc 'Support task that emits a copy of import scripts in Directory specified by IMPORTS_OUTPUT_DIR.'
      task "#{task_prefix}:emit_standard_imports" => ["#{task_prefix}:prepare"] do
        base_dir = ENV['IMPORTS_OUTPUT_DIR'] || 'tmp/imports'

        Dbt.banner("Emit standard imports to IMPORTS_OUTPUT_DIR=#{base_dir}", key)

        repository = get_domgen_repository(repository_key)

        repository.data_modules.select { |data_module| data_module.sql? }.each do |data_module|
          data_module.entities.select{|entity| entity.sql? && entity.concrete? && !entity.sql.load_from_fixture? }.each do |entity|
            file = File.expand_path("#{base_dir}/#{data_module.name}/import/#{entity.sql.qualified_table_name.to_s.gsub('[','').gsub(']','').gsub('"','')}.sql")
            FileUtils.mkdir_p File.dirname(file)
            File.open(file, 'wb') do |f|
              f.write <<-SQL
INSERT INTO [@@TARGET@@].#{entity.sql.qualified_table_name}(#{entity.attributes.select{|a|a.sql?}.collect{|a|a.sql.quoted_column_name }.join(', ')})
  SELECT #{entity.attributes.select{|a|a.sql?}.collect{|a|a.sql.quoted_column_name }.join(', ')}
  FROM [@@SOURCE@@].#{entity.sql.qualified_table_name}
              SQL
            end
          end
        end
      end
    end

    def add_pre_db_artifacts(*artifacts)
      ::Buildr.artifacts(artifacts).each do |a|
        self.pre_db_artifacts << a.to_s
        task "#{self.task_prefix}:pre_build" => [a]
      end
    end

    def add_post_db_artifacts(*artifacts)
      ::Buildr.artifacts(artifacts).each do |a|
        self.post_db_artifacts << a.to_s
        task "#{self.task_prefix}:pre_build" => [a]
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

    private

    def get_domgen_repository(repository_key)
      if repository_key
        repository = Domgen.repository_by_name(repository_key)
        if Domgen.repositories.size == 1
          Domgen.warn("Dbt database #{key} specifies a repository_key parameter in the domgen integration but it can be be derived as there is only a single repository. The parameter should be removed.")
        end
        return repository
      elsif repository_key.nil?
        repositories = Domgen.repositories
        if repositories.size == 1
          return repositories[0]
        else
          Domgen.error("Dbt database #{key} does not specify a repository_key parameter and it can not be derived. Candidate repositories include #{repositories.collect { |r| r.name }.inspect}")
        end
      end
    end
  end

  private

  def self.define_tasks_for_artifact_database(database, artifact)
    self.define_common_tasks_for_database(database)

    extra_actions = database.extra_actions
    (%w(create drop) + extra_actions).each do |action|
      desc "#{action} the #{database.key} database"
      task "#{database.task_prefix}:#{action}" => ["#{database.task_prefix}:load_config"] do
        banner("Running #{action} on package", database.key)
        a = ::Buildr.artifact(artifact)
        a.invoke
        Java::Commands.java '-jar',
                            a.to_s,
                            '--database',
                            database.key,
                            '--environment',
                            Dbt::Config.environment,
                            '--config-file',
                            Dbt::Config.config_filename,
                            action
      end
    end
  end

  def self.define_common_tasks_for_database(database)
    self.define_basic_tasks
    task "#{database.task_prefix}:load_config" => ["#{Dbt::Config.task_prefix}:global:load_config"]

    task "#{database.task_prefix}:pre_build" => ["#{Dbt::Config.task_prefix}:all:pre_build"]

    task "#{database.task_prefix}:prepare_fs" => ["#{database.task_prefix}:pre_build"]

    task "#{database.task_prefix}:prepare" => ["#{database.task_prefix}:load_config", "#{database.task_prefix}:prepare_fs"]
  end

  def self.define_tasks_for_database(database)
    self.define_common_tasks_for_database(database)

    task "#{database.task_prefix}:prepare_fs" do
      @@runtime.load_database_config(database)
    end

    desc "Create the #{database.key} database."
    task "#{database.task_prefix}:create" => ["#{database.task_prefix}:prepare"] do
      banner('Creating database', database.key)
      @@runtime.create(database)
    end

    desc "Drop the #{database.key} database."
    task "#{database.task_prefix}:drop" => ["#{database.task_prefix}:load_config"] do
      banner('Dropping database', database.key)
      @@runtime.drop(database)
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
      desc 'Apply migrations to bring data to latest version'
      task "#{database.task_prefix}:migrate" => ["#{database.task_prefix}:prepare"] do
        banner('Migrating', database.key)
        @@runtime.migrate(database)
      end
    end

    # Import tasks
    if database.enable_separate_import_task?
      database.imports.values.each do |imp|
        define_import_task("#{database.task_prefix}", imp, 'contents')
      end
    end

    database.module_groups.values.each do |module_group|
      define_module_group_tasks(module_group)
    end

    if database.enable_import_task_as_part_of_create?
      database.imports.values.each do |imp|
        key = ''
        key = ':' + imp.key.to_s unless Dbt::Config.default_import?(imp.key)
        desc "Create the #{database.key} database by import."
        task "#{database.task_prefix}:create_by_import#{key}" => ["#{database.task_prefix}:prepare"] do
          banner("Creating Database By Import#{Dbt::Config.default_import?(imp.key) ? '' : " (#{imp.key})"}", database.key)
          @@runtime.create_by_import(imp)
        end
      end
    end

    if database.backup?
      desc "Perform backup of #{database.key} database"
      task "#{database.task_prefix}:backup" => ["#{database.task_prefix}:load_config"] do
        banner('Backing up Database', database.key)
        @@runtime.backup(database)
      end
    end

    if database.restore?
      desc "Perform restore of #{database.key} database"
      task "#{database.task_prefix}:restore" => ["#{database.task_prefix}:load_config"] do
        banner('Restoring Database', database.key)
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
    unless @@defined_init_tasks
      task "#{Dbt::Config.task_prefix}:global:load_config" do
        unless @@repository.load_configuration_data
          raise 'unable to load database configuration data.'
        end
      end

      task "#{Dbt::Config.task_prefix}:all:pre_build"

      @@defined_init_tasks = true
    end
  end

  def self.banner(message, database_key)
    config_key = Dbt.runtime.config_key_for_database(Dbt.database_for_key(database_key), Dbt::Config.environment)
    catalog_name = Dbt.repository.configuration_for_key?(config_key) ? Dbt.configuration_for_key(database_key).catalog_name : ''
    @@runtime.info("**** #{message}: (Database: #{database_key}, Environment: #{Dbt::Config.environment}, Catalog: #{catalog_name}) ****")
  end
end
