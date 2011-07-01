class DbTasks
  class PostgresDbConfig < ActiveRecordDbConfig
  end

  class PostgresDbDriver < ActiveRecordDbDriver
    def create_schema(schema_name)
      if select_rows("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
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

    def pre_table_import(imp, module_name, table)
    end

    def post_table_import(imp, module_name, table)
    end

    def post_data_module_import(imp, module_name)
    end

    protected

    def current_database
      select_value("SELECT current_database()")
    end

    def control_database_name
      'postgres'
    end
  end
end
