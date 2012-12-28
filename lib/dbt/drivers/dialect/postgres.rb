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
  module Dialect
    module Postgres

      CONTROL_DATABASE = "postgres"

      def create_schema(schema_name)
        if query("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
          execute("CREATE SCHEMA #{quote_table_name(schema_name)}")
        end
      end

      def drop_schema(schema_name, tables)
        # TODO: Drop dependents
        execute("DROP SCHEMA #{quote_table_name(schema_name)}")
      end

      def create_database(database, configuration)
        execute("CREATE DATABASE #{quote_table_name(configuration.catalog_name)}")
        unless database.version.nil?
          execute("COMMENT ON DATABASE #{quote_table_name(configuration.catalog_name)} IS 'Database Schema Version #{database.version}'")
        end
      end

      def drop(database, configuration)
        execute("DROP DATABASE IF EXISTS #{quote_table_name(configuration.catalog_name)}")
      end

      def backup(database, configuration)
        raise NotImplementedError
      end

      def restore(database, configuration)
        raise NotImplementedError
      end

      def pre_fixture_import(table)
      end

      def post_fixture_import(table)
      end

      def pre_table_import(imp, table)
      end

      def post_table_import(imp, table)
      end

      def post_data_module_import(imp, module_name)
      end

      def post_database_import(imp)
      end

      protected

      def current_database
        select_value("SELECT current_database()")
      end

      def select_database(database_name)
        if database_name.nil?
          execute('\\connect "postgres"')
        else
          execute("\\connect \"#{database_name}\"")
        end
      end

      def quote_table_name(name)
        "\"#{name}\""
      end

    end
  end
end