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

class Dbt
  class AbstractDbConfig < Dbt::DbConfig

    def initialize(configuration)
      @configuration = configuration
    end

    attr_reader :configuration

    def no_create?
      true == config_value("no_create", true)
    end

    def catalog_name
      config_value("database", false)
    end

    def host
      config_value("host", false)
    end

    def username
      config_value("username", true)
    end

    def password
      config_value("password", true)
    end

    protected

    def config_value(config_param_name, allow_nil)
      value = self.configuration[config_param_name]
      raise "Unable to locate configuration value named #{config_param_name}" if !allow_nil && value.nil?
      value
    end
  end
end
