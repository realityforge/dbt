class DbTasks
  class PostgresDbConfig < ActiveRecordDbConfig
  end

  class PostgresDbDriver < ActiveRecordDbDriver
    def create_schema(schema_name)
      if ActiveRecord::Base.connection.select_all("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{schema_name}'").empty?
        execute("CREATE SCHEMA \"#{schema_name}\"")
      end
    end

    def drop_schema(schema_name, tables)
      execute("DROP SCHEMA \"#{schema_name}\"")
    end

    def create_database(database, configuration)
      execute(<<SQL)
CREATE DATABASE #{configuration.catalog_name}
SQL
    end

    def drop(database, configuration)
      unless ActiveRecord::Base.connection.select_all("SELECT * FROM pg_catalog.pg_database WHERE datname = '#{configuration.catalog_name}'").empty?
        execute("DROP DATABASE #{configuration.catalog_name}")
      end
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
      ActiveRecord::Base.connection.select_value("SELECT DB_NAME()")
    end

    def control_database_name
      'postgres'
    end
  end
end
