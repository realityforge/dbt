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
require 'yaml'
require 'erb'

class Dbt #nodoc

  class Repository

    def initialize
      @databases = {}
      @configurations = {}
      @configuration_data = {}
    end

    def database_keys
      @databases.keys
    end

    def database_for_key?(database_key)
      !@databases[database_key.to_s].nil?
    end

    def database_for_key(database_key)
      database = @databases[database_key.to_s]
      raise "Missing database for key #{database_key}" unless database
      database
    end

    def add_database(database_key, options = {}, &block)
      raise "Database with key #{database_key} already defined." if @databases.has_key?(database_key.to_s)

      database = DatabaseDefinition.new(database_key, options, &block)
      @databases[database_key.to_s] = database

      database
    end

    def remove_database(database_key)
      raise "Database with key #{database_key} not defined." unless @databases.has_key?(database_key.to_s)
      @databases.delete(database_key.to_s)
    end

    def configuration_for_key?(config_key)
      !!@configuration_data[config_key.to_s]
    end

    def configuration_keys
      @configuration_data.keys
    end

    def configuration_for_key(config_key)
      existing = @configurations[config_key.to_s]
      return existing if existing
      c = @configuration_data[config_key.to_s]
      if c.nil?
        Dbt.runtime.info("Dbt unable to locate configuration for key '#{config_key}'.")
        if 0 == @configuration_data.size
          Dbt.runtime.info(<<MSG)
Dbt has not loaded configuration. The configuration is expected to be loaded from the file '#{Dbt::Config.config_filename}'. Ensure that either the '#{Dbt::Config.task_prefix}:global:load_config' rake task is executed or that the method `Dbt.repository.load_configuration_data` is invoked prior to this point.
MSG
        else
          Dbt.runtime.info(<<MSG)
Configuration has been loaded from the file '#{Dbt::Config.config_filename}' but no entry for '#{config_key}' is present in the file.
MSG
        end
      end
      raise "Missing database configuration for key '#{config_key}'" unless c
      add_configuration(config_key, c)
    end

    def add_configuration(config_key, config, &block)
      Dbt.error("Attempting to redefine configuration for key '#{config_key}'.") if @configurations[config_key.to_s]
      @configurations[config_key.to_s] = Dbt::Config.driver_config(config_key, config, &block)
    end

    def is_configuration_data_loaded?
      !@configuration_data.empty?
    end

    def ensure_configuration_file_present(filename)
      unless File.exist?(filename)
        example_filename = Dbt::Config.example_config_filename
        if example_filename
          if File.exist?(example_filename)
            Dbt.runtime.info("Copying sample configuration file from #{example_filename} to #{filename}")
            FileUtils.cp example_filename, filename
          end
        end
      end
    end

    def load_configuration_data(filename = Dbt::Config.config_filename)
      return true if is_configuration_data_loaded?

      ensure_configuration_file_present(filename)

      filename = File.expand_path(filename, Dbt::Config.base_directory)
      if File.exist?(filename)
        begin
          self.configuration_data = YAML::load(ERB.new(IO.read(filename)).result)
        rescue Exception => e
          Dbt.runtime.info("Dbt unable to load database configuration from #{filename}. Cause: #{e}")
          return false
        end
      else
        Dbt.runtime.info("Dbt unable to load database configuration from #{filename} as file does not exist.")
        return false
      end
    end

    def configuration_data
      @configuration_data.dup
    end

    def configuration_data=(configuration_data)
      @configurations = {}
      @configuration_data = configuration_data.nil? ? {} : configuration_data
    end
  end
end
