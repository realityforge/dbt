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

  class RepositoryDefinition < Reality::BaseElement
    attr_accessor :schema_overrides, :table_map, :sequence_map

    def initialize(options = {}, &block)
      @table_map = {}
      @sequence_map = {}
      @schema_overrides = {}
      @modules = []
      super(options, &block)
    end

    attr_writer :modules

    # List of modules to process for database
    def modules
      @modules = @modules.call if !@modules.nil? && @modules.is_a?(Proc)
      @modules
    end

    def schema_name_for_module(module_name)
      override = schema_overrides[module_name]
      return override if override
      return module_name if self.modules.include?(module_name)
      raise "Unable to determine schema name for non existent module #{module_name}"
    end

    def ordered_elements_for_module(module_name)
      table_ordering(module_name).dup + sequence_ordering(module_name).dup
    end

    def table_ordering(module_name)
      tables = table_map[module_name.to_s]
      raise "No tables defined for module #{module_name}" unless tables
      tables
    end

    def table_ordering?(module_name)
      !!table_map[module_name.to_s]
    end

    def sequence_ordering(module_name)
      sequences = sequence_map[module_name.to_s]
      raise "No sequences defined for module #{module_name}" unless sequences
      sequences
    end

    def sequence_ordering?(module_name)
      !!sequence_map[module_name.to_s]
    end

    def merge!(other)
      other.modules.each do |m|
        if self.modules.include?(m)
          raise "Attempting to merge repository that has duplicate module definition #{m} (Existing = #{self.modules.inspect})"
        end
      end
      other.modules.each do |m|
        self.modules.push(m)
      end
      self.schema_overrides.merge!(other.schema_overrides)
      self.table_map.merge!(other.table_map)
      self.sequence_map.merge!(other.sequence_map)
      self
    end

    def from_yaml(content)
      require 'yaml'
      repository_config = YAML::load(content)
      # keys method only available in ruby 1.9
      modules = repository_config['modules'].nil? ? [] : (repository_config['modules'].respond_to?(:keys) ? repository_config['modules'].keys : repository_config['modules'].map{|k,v| k})
      schema_overrides = {}
      table_map = {}
      sequence_map = {}
      repository_config['modules'].each do |module_config|
        name = module_config[0]
        schema = module_config[1]['schema']
        tables = module_config[1]['tables']
        sequences = module_config[1]['sequences'] || []
        table_map[name] = tables
        sequence_map[name] = sequences
        schema_overrides[name] = schema if name != schema
      end if repository_config['modules']

      self.modules = modules
      self.schema_overrides = schema_overrides
      self.table_map = table_map
      self.sequence_map = sequence_map
      self
    end

    def to_yaml
      yaml = "---\nmodules: !omap"
      return yaml + " []\n" if self.modules.size == 0
      yaml += "\n"
      self.modules.each do |module_name|
        yaml += "   - #{module_name}:\n"
        yaml += "      schema: #{self.schema_name_for_module(module_name)}\n"
        tables = self.table_ordering?(module_name) ? self.table_ordering(module_name) : []
        yaml += "      tables:#{tables.empty? ? ' []' : ''}\n"
        tables.each do |table_name|
          quoted_table = table_name =~ /"/ ? "'#{table_name}'" : "\"#{table_name}\""
          yaml += "        - #{quoted_table}\n"
        end
        sequences = self.sequence_ordering?(module_name) ? self.sequence_ordering(module_name) : []
        yaml += "      sequences:#{sequences.empty? ? ' []' : ''}\n"
        sequences.each do |sequence_name|
          quoted_sequence = sequence_name =~ /"/ ? "'#{sequence_name}'" : "\"#{sequence_name}\""
          yaml += "        - #{quoted_sequence}\n"
        end
      end
      yaml
    end
  end
end
