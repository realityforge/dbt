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

class Dbtc
  class << self
    def postgres?
      Dbt::Config.driver == 'postgres'
    end

    def sql_server?
      Dbt::Config.driver == 'sql_server'
    end

    def db_prefix(*parts)
      parts.compact.collect { |p| "#{p}_" }.join('')
    end

    def env(key, default_value = nil)
      ENV[key] || default_value || (raise "Unable to locate environment variable #{key}")
    end
  end
end
