require 'dbt/drivers/dialect/postgres'

class Dbt
  class PostgresDbConfig < JdbcDbConfig
  end

  class PostgresDbDriver < JdbcDbDriver
    include Dbt::Dialect::Postgres
  end
end
