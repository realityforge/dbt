class Dbt
  class JdbcDbConfig < Dbt::DbConfig
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

    def jdbc_driver
      raise NotImplementedError
    end

    def jdbc_url(use_control_catalog)
      raise NotImplementedError
    end

    def jdbc_info
      raise NotImplementedError
    end

    protected

    def config_value(config_param_name, allow_nil)
      value = self.configuration[config_param_name]
      raise "Unable to locate configuration value named #{config_param_name}" if !allow_nil && value.nil?
      value
    end
  end

  class JdbcDbDriver < Dbt::DbDriver

    def execute(sql, execute_in_control_database = false)
      raise "Can not execute statement when database connection is not open." unless open?
      current_database = nil
      if execute_in_control_database
        current_database = self.current_database
        select_database(nil)
      end
      statement = @connection.createStatement()
      statement.executeUpdate(sql)
      statement.close
      select_database(current_database) if execute_in_control_database
    end

    def insert_row(table_name, row)
      column_names = row.keys.collect { |column_name| quote_column_name(column_name) }
      value_list = row.values.collect { |value| quote_value(value) }
      execute("INSERT INTO #{table_name} (#{column_names.join(', ')}) VALUES (#{value_list.join(', ')})")
    end

    def select_rows(sql)
      select_data(sql)
    end

    def column_names_for_table(table)
      ActiveRecord::Base.connection.columns(table).collect { |c| quote_column_name(c.name) }
    end

    def open(config, use_control_database)
      raise "Can not open database connection. Connection already open." if open?
      java.lang.Class.forName(config.jdbc_driver, true, java.lang.Thread.currentThread.getContextClassLoader) if config.jdbc_driver
      @connection = java.sql.DriverManager.getConnection(config.jdbc_url(use_control_database), config.jdbc_info)
      select_database(nil) if use_control_database
    end

    def close
      if open?
        @connection.close() rescue Exception
        @connection = nil
      end
    end

    protected

    def select_one(sql)
      result = select_data(sql)
      result.first if result
    end

    # Returns an array of the values of the first column in a select:
    #   select_values("SELECT id FROM companies LIMIT 3") => [1,2,3]
    def select_values(sql)
      select_data(sql).map { |v| v[0] }
    end

    # Returns a single value from a record
    def select_value(sql)
      if result = select_one(sql)
        result.values.first
      end
    end

    def select_data(sql)
      statement = @connection.createStatement()
      statement.executeQuery(sql)
      column_names = []

      rs = statement.executeQuery(sql)
      meta_data = rs.getMetaData()
      (1..meta_data.columnCount).each do |index|
        column_names << meta_data.getColumnName(index)
      end

      results = []

      while rs.next()
        result = Dbt::OrderedHash.new

        column_names.each_with_index do |name, index|
          result[name] = rs.getObject(index + 1)
        end
        results << result
      end
      rs.close
      statement.close
      results
    end

    def open?
      !@connection.nil?
    end
  end
end
