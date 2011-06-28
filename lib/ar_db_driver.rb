class DbTasks
  class ActiveRecordDbConfig < DbTasks::DbConfig
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

    protected

    def config_value(config_param_name, allow_nil)
      value = self.configuration[config_param_name]
      raise "Unable to locate configuration value named #{config_param_name}" if !allow_nil && value.nil?
      value
    end
  end

  class ActiveRecordDbDriver < DbTasks::DbDriver
    def execute(sql, execute_in_control_database = false)
      current_database = nil
      if execute_in_control_database
        current_database = self.current_database
        select_database(nil)
      end
      ActiveRecord::Base.connection.execute(sql, nil)
      select_database(current_database) if execute_in_control_database
    end

    def insert_row(table_name, row)
      column_names = row.keys.collect { |column_name| quote_column_name(column_name) }
      value_list = row.values.collect { |value| quote_value(value).gsub('[^\]\\n', "\n").gsub('[^\]\\r', "\r") }
      execute("INSERT INTO #{table_name} (#{column_names.join(', ')}) VALUES (#{value_list.join(', ')})")
    end

    def select_rows(sql)
      #TODO: Currently does not return times correctly. This needs to be fixed for fixture dumping to work
      ActiveRecord::Base.connection.select_rows(sql, nil)
    end

    def column_names_for_table(table)
      ActiveRecord::Base.connection.columns(table).collect { |c| quote_column_name(c.name) }
    end

    def open(config, open_control_database, log_filename)
      require 'active_record'
      raise "Can not open database connection. Connection already open." if open?
      ActiveRecord::Base.colorize_logging = false
      connection_config = config.configuration.dup
      connection_config['database'] = self.control_database_name if open_control_database
      ActiveRecord::Base.establish_connection(connection_config)
      FileUtils.mkdir_p File.dirname(log_filename)
      ActiveRecord::Base.logger = Logger.new(File.open(log_filename, 'a'))
      ActiveRecord::Migration.verbose = ENV["VERBOSE"] ? ENV["VERBOSE"] == "true" : false
    end

    def close
      ActiveRecord::Base.connection.disconnect! if open?
    end

    protected

    def select_value(sql)
      ActiveRecord::Base.connection.select_value(sql)
    end

    def open?
      ActiveRecord::Base.connection && ActiveRecord::Base.connection.active? rescue false
    end

    def quote_column_name(column_name)
      ActiveRecord::Base.connection.quote_column_name(column_name)
    end

    def quote_value(value)
      ActiveRecord::Base.connection.quote(value)
    end
  end
end
