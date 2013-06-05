require File.expand_path('../helper', __FILE__)

class TestRuntimeBasic < Dbt::TestCase

  def test_create
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['foo', 'bar', 'baz']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    mock.expects(:open).with(config, true)
    mock.expects(:drop).with(database, config)
    mock.expects(:close).with()
    mock.expects(:open).with(config, false)
    mock.expects(:create_database).with(database, config)
    mock.expects(:create_schema).with(module_name)
    mock.expects(:close).with()

    Dbt.runtime.create(database)
  end

  def test_create_with_fixtures
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[foo]', '[bar]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    create_fixture(module_name, 'foo')

    # Look at me - I am not a fixture!
    # TODO: Presence of this file should generate an error?
    create_file("databases/#{module_name}/fixtures/bar.sql", "SELECT * FROM tblNotRun")

    mock.expects(:open).with(config, true)
    mock.expects(:drop).with(database, config)
    mock.expects(:close).with()
    mock.expects(:open).with(config, false)
    mock.expects(:create_database).with(database, config)
    mock.expects(:create_schema).with(module_name)
    expect_fixture(mock, 'foo')
    mock.expects(:close).with()

    Dbt.runtime.create(database)
  end

  def test_create_with_sql
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[foo]', '[bar]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    Dbt::Config.default_up_dirs = ['.', 'Dir1', 'Dir2']
    Dbt::Config.default_finalize_dirs = ['Dir3', 'Dir4']
    Dbt::Config.fixture_dir_name = 'foo'

    create_table_sql("#{module_name}", 'a')
    create_table_sql("#{module_name}", 'b')
    create_table_sql("#{module_name}/Dir1", 'd')
    create_table_sql("#{module_name}/Dir1", 'c')
    create_table_sql("#{module_name}/Dir2", 'e')
    create_table_sql("#{module_name}/Dir2", 'f')
    create_fixture(module_name, 'foo')
    create_table_sql("#{module_name}/Dir3", 'g')
    create_table_sql("#{module_name}/Dir4", 'h')

    mock.expects(:open).with(config, true)
    mock.expects(:drop).with(database, config)
    mock.expects(:close).with()
    mock.expects(:open).with(config, false)
    mock.expects(:create_database).with(database, config)
    mock.expects(:create_schema).with(module_name)
    expect_create_table(mock, module_name, '', 'a')
    expect_create_table(mock, module_name, '', 'b')
    expect_create_table(mock, module_name, 'Dir1/', 'd')
    expect_create_table(mock, module_name, 'Dir1/', 'c')
    expect_create_table(mock, module_name, 'Dir2/', 'e')
    expect_create_table(mock, module_name, 'Dir2/', 'f')
    expect_fixture(mock, 'foo')
    expect_create_table(mock, module_name, 'Dir3/', 'g')
    expect_create_table(mock, module_name, 'Dir4/', 'h')
    mock.expects(:close).with()

    Dbt.runtime.create(database)
  end

  def test_drop
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['foo', 'bar', 'baz']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    mock.expects(:open).with(config, true)
    mock.expects(:drop).with(database, config)
    mock.expects(:close).with()

    Dbt.runtime.drop(database)
  end

  def create_table_sql(dir, table_name)
    create_file("databases/#{dir}/#{table_name}.sql", "CREATE TABLE [#{table_name}]")
  end

  def create_fixture(module_name, table_name)
    create_file("databases/#{module_name}/#{Dbt::Config.fixture_dir_name}/#{table_name}.yml", "1:\n  ID: 1\n")
  end

  def expect_create_table(mock, module_name, dirname, table_name)
    mock.expects(:execute).with("CREATE TABLE [#{table_name}]", false)
    Dbt.runtime.expects(:info).with("#{'%-15s' % module_name}: #{dirname}#{table_name}.sql")
  end

  def expect_fixture(mock, table_name)
    mock.expects(:execute).with("DELETE FROM [#{table_name}]", false)
    mock.expects(:pre_fixture_import).with("[#{table_name}]")
    mock.expects(:insert).with("[#{table_name}]", 'ID' => 1)
    mock.expects(:post_fixture_import).with("[#{table_name}]")
    Dbt.runtime.expects(:info).with("Fixture        : #{table_name}")
  end

  def create_simple_db_definition(db_scripts, module_name, table_names)
    Dbt.add_database(:default) do |db|
      db.rake_integration = false
      db.modules = [module_name]
      db.table_map = {module_name => table_names}
      db.search_dirs = [db_scripts]
    end
  end

  def create_postgres_config
    Dbt::Config.driver = 'Pg'
    Dbt.repository.configuration_data = {
      Dbt::Config.environment =>
        {
          'database' => 'DBT_TEST',
          'username' => ENV['USER'],
          'password' => 'letmein',
          'host' => '127.0.0.1',
          'port' => 5432
        }
    }
    Dbt.repository.configuration_for_key(Dbt::Config.environment)
  end
end
