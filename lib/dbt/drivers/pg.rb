require 'dbt/drivers/dialect/postgres'
require 'dbt/abstract_db_config'

class Dbt
  class PgDbConfig < AbstractDbConfig
  end

  class PgDbDriver < Dbt::DbDriver
    include Dbt::Dialect::Postgres

    def open(config, use_control_database)
      raise "Can not open database connection. Connection already open." if open?
      raise "Expected adapter = 'postgresql' but got '#{config.configuration["adapter"]}'." unless config.configuration["adapter"].eql?('postgresql')

      database = use_control_database ? CONTROL_DATABASE : config.configuration["database"]

      @connection = PG.connect(:host => "localhost",
                               :port => "5432",
                               :dbname => database,
                               :user => config.configuration["username"],
                               :password => config.configuration["password"])
    end

    def execute(sql, execute_in_control_database = false)
      raise "Can not execute statement when database connection is not open." unless open?
      current_database = nil
      if execute_in_control_database
        current_database = self.current_database
        select_database(nil)
      end
      success = false
      begin
        @connection.exec(sql)
        success = true
      ensure
        puts "Failed SQL: #{sql}" unless success
        select_database(current_database) if execute_in_control_database
      end
    end

    def query(sql)
      rs = @connection.exec(sql)

      return [] if rs.ntuples == 0

      results = []

      rs.each do |rs_hash|
        result = Dbt::OrderedHash.new

        rs_hash.each do |key, value|
          result[key] = value
        end
        results << result
      end
      results
    end

    def close
      if open?
        @connection.close() rescue Exception
        @connection = nil
      end
    end

    protected

    def open?
      !@connection.nil?
    end

    def quote_table_name(name)
      PG::Connection.quote_ident(name)
    end
  end
end
