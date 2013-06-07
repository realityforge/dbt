require File.expand_path('../helper', __FILE__)

class TestRuntimeBasic < Dbt::TestCase

  def test_create
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]', '[MyModule].[bar]', '[MyModule].[baz]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    mock.expects(:open).with(config, true).in_sequence(@s)
    mock.expects(:drop).with(database, config).in_sequence(@s)
    mock.expects(:create_database).with(database, config).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)
    mock.expects(:open).with(config, false).in_sequence(@s)
    mock.expects(:create_schema).with(module_name).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.create(database)
  end

  def test_create_by_import
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config({}, 'import' => base_postgres_config().merge('database' => 'IMPORT_DB'))

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)
    database.separate_import_task = true
    import = database.add_import(:default, {})

    mock.expects(:open).with(config, true).in_sequence(@s)
    mock.expects(:drop).with(database, config).in_sequence(@s)
    mock.expects(:create_database).with(database, config).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)
    mock.expects(:open).with(config, false).in_sequence(@s)
    mock.expects(:create_schema).with(module_name).in_sequence(@s)
    expect_default_table_import(mock, import, module_name, 'foo')
    mock.expects(:post_data_module_import).with(import, module_name).in_sequence(@s)
    mock.expects(:post_database_import).with(import).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.create_by_import(import)
  end

  def test_create_with_no_create
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config('no_create' => true)

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]', '[MyModule].[bar]', '[MyModule].[baz]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    mock.expects(:open).with(config, false).in_sequence(@s)
    mock.expects(:create_schema).with(module_name).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.create(database)
  end

  def test_create_with_multiple_fixtures
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]', '[MyModule].[bar]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    create_fixture(module_name, 'foo')
    create_fixture(module_name, 'bar')

    mock.expects(:open).with(config, true).in_sequence(@s)
    mock.expects(:drop).with(database, config).in_sequence(@s)
    mock.expects(:create_database).with(database, config).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)
    mock.expects(:open).with(config, false).in_sequence(@s)
    mock.expects(:create_schema).with(module_name).in_sequence(@s)
    expect_delete(mock, module_name, 'bar')
    expect_delete(mock, module_name, 'foo')
    expect_fixture(mock, module_name, 'foo')
    expect_fixture(mock, module_name, 'bar')
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.create(database)
  end

  def test_create_with_unexpected_fixtures
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]', '[MyModule].[bar]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    create_fixture(module_name, 'baz')

    mock.expects(:open).with(config, true).in_sequence(@s)
    mock.expects(:drop).with(database, config).in_sequence(@s)
    mock.expects(:create_database).with(database, config).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)
    mock.expects(:open).with(config, false).in_sequence(@s)
    mock.expects(:create_schema).with(module_name).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)

    assert_raises(RuntimeError) do
      Dbt.runtime.create(database)
    end
  end

  def test_create_with_fixtures
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]', '[MyModule].[bar]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    create_fixture(module_name, 'foo')

    mock.expects(:open).with(config, true).in_sequence(@s)
    mock.expects(:drop).with(database, config).in_sequence(@s)
    mock.expects(:create_database).with(database, config).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)
    mock.expects(:open).with(config, false).in_sequence(@s)
    mock.expects(:create_schema).with(module_name).in_sequence(@s)
    expect_delete(mock, module_name, 'foo')
    expect_fixture(mock, module_name, 'foo')
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.create(database)
  end

  def test_create_with_fixtures_including_non_fixture
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[foo]', '[bar]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    create_file("databases/#{module_name}/fixtures/bar.sql", "SELECT * FROM tblNotRun")

    mock.expects(:open).with(config, true).in_sequence(@s)
    mock.expects(:drop).with(database, config).in_sequence(@s)
    mock.expects(:create_database).with(database, config).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)
    mock.expects(:open).with(config, false).in_sequence(@s)
    mock.expects(:create_schema).with(module_name).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)

     assert_raises(RuntimeError) do
      Dbt.runtime.create(database)
    end
  end

  def test_create_with_sql
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    Dbt::Config.default_up_dirs = ['.', 'Dir1', 'Dir2']
    Dbt::Config.default_finalize_dirs = ['Dir3', 'Dir4']
    Dbt::Config.fixture_dir_name = 'foo'
    Dbt::Config.default_pre_create_dirs = ['db-pre-create']
    Dbt::Config.default_post_create_dirs = ['db-post-create']

    create_table_sql("db-pre-create", 'preCreate')
    create_table_sql("#{module_name}", 'a')
    create_table_sql("#{module_name}", 'b')
    create_table_sql("#{module_name}/Dir1", 'd')
    create_table_sql("#{module_name}/Dir1", 'c')
    create_table_sql("#{module_name}/Dir2", 'e')
    create_table_sql("#{module_name}/Dir2", 'f')
    create_fixture(module_name, 'foo')
    create_table_sql("#{module_name}/Dir3", 'g')
    create_table_sql("#{module_name}/Dir4", 'h')
    create_table_sql("db-post-create", 'postCreate')

    mock.expects(:open).with(config, true).in_sequence(@s)
    mock.expects(:drop).with(database, config).in_sequence(@s)
    mock.expects(:create_database).with(database, config).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)
    mock.expects(:open).with(config, false).in_sequence(@s)
    expect_create_table(mock, '', 'db-pre-create/', 'preCreate')
    mock.expects(:create_schema).with(module_name).in_sequence(@s)
    expect_create_table(mock, module_name, '', 'a')
    expect_create_table(mock, module_name, '', 'b')
    expect_create_table(mock, module_name, 'Dir1/', 'c')
    expect_create_table(mock, module_name, 'Dir1/', 'd')
    expect_create_table(mock, module_name, 'Dir2/', 'e')
    expect_create_table(mock, module_name, 'Dir2/', 'f')
    expect_delete(mock, module_name, 'foo')
    expect_fixture(mock, module_name, 'foo')
    expect_create_table(mock, module_name, 'Dir3/', 'g')
    expect_create_table(mock, module_name, 'Dir4/', 'h')
    expect_create_table(mock, '', 'db-post-create/', 'postCreate')
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.create(database)
  end

  def test_drop
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config()

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]', '[MyModule].[bar]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)

    mock.expects(:open).with(config, true).in_sequence(@s)
    mock.expects(:drop).with(database, config).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.drop(database)
  end

  def test_import
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config({}, 'import' => base_postgres_config().merge('database' => 'IMPORT_DB'))

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]', '[MyModule].[bar]', '[MyModule].[baz]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)
    database.separate_import_task = true
    import = database.add_import(:default, {})

    mock.expects(:open).with(config, false).in_sequence(@s)
    expect_delete_for_table_import(mock, module_name, 'baz')
    expect_delete_for_table_import(mock, module_name, 'bar')
    expect_delete_for_table_import(mock, module_name, 'foo')
    expect_default_table_import(mock, import, module_name, 'foo')
    expect_default_table_import(mock, import, module_name, 'bar')
    expect_default_table_import(mock, import, module_name, 'baz')
    mock.expects(:post_data_module_import).with(import, module_name).in_sequence(@s)
    mock.expects(:post_database_import).with(import).in_sequence(@s)

    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.database_import(database.import_by_name(:default), nil)
  end

  def test_import_with_IMPORT_RESUME_AT
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config({}, 'import' => base_postgres_config().merge('database' => 'IMPORT_DB'))

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]', '[MyModule].[bar]', '[MyModule].[baz]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)
    database.separate_import_task = true
    import = database.add_import(:default, {})

    mock.expects(:open).with(config, false).in_sequence(@s)
    expect_delete_for_table_import(mock, module_name, 'bar')
    expect_default_table_import(mock, import, module_name, 'bar')
    expect_default_table_import(mock, import, module_name, 'baz')
    mock.expects(:post_data_module_import).with(import, module_name).in_sequence(@s)
    mock.expects(:post_database_import).with(import).in_sequence(@s)

    mock.expects(:close).with()

    ENV['IMPORT_RESUME_AT'] = 'MyModule.bar'
    Dbt.runtime.database_import(database.import_by_name(:default), nil)
  end

  def test_import_by_sql
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config({}, 'import' => base_postgres_config().merge('database' => 'IMPORT_DB'))

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)
    database.separate_import_task = true
    import = database.add_import(:default, {})

    Dbt::Config.default_import_dir = 'zzzz'
    import_sql = "INSERT INTO DBT_TEST.[foo]"
    create_file("databases/#{module_name}/zzzz/MyModule.foo.sql", import_sql)

    mock.expects(:open).with(config, false).in_sequence(@s)
    expect_delete_for_table_import(mock, module_name, 'foo')
    expect_pre_table_import(mock, import, module_name, 'foo', 'S')
    mock.expects(:execute).with(import_sql, true).in_sequence(@s)
    expect_post_table_import(mock, import, module_name, 'foo')
    mock.expects(:post_data_module_import).with(import, module_name).in_sequence(@s)
    mock.expects(:post_database_import).with(import).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.database_import(database.import_by_name(:default), nil)
  end

  def test_import_by_fixture
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config({}, 'import' => base_postgres_config().merge('database' => 'IMPORT_DB'))

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)
    database.separate_import_task = true
    import = database.add_import(:default, {})

    Dbt::Config.default_import_dir = 'zzzz'
    fixture_data = "1:\n  ID: 1\n"
    create_file("databases/MyModule/zzzz/MyModule.foo.yml", fixture_data)

    mock.expects(:open).with(config, false).in_sequence(@s)
    expect_delete_for_table_import(mock, module_name, 'foo')
    expect_pre_table_import(mock, import, module_name, 'foo', 'F')
    mock.expects(:pre_fixture_import).with('[MyModule].[foo]').in_sequence(@s)
    mock.expects(:insert).with('[MyModule].[foo]', 'ID' => 1).in_sequence(@s)
    mock.expects(:post_fixture_import).with('[MyModule].[foo]').in_sequence(@s)
    expect_post_table_import(mock, import, module_name, 'foo')
    mock.expects(:post_data_module_import).with(import, module_name).in_sequence(@s)
    mock.expects(:post_database_import).with(import).in_sequence(@s)
    mock.expects(:close).with().in_sequence(@s)

    Dbt.runtime.database_import(database.import_by_name(:default), nil)
  end

  def test_import_using_pre_post_dirs
    mock = Dbt::DbDriver.new
    Dbt.runtime.instance_variable_set("@db", mock)

    config = create_postgres_config({}, 'import' => base_postgres_config().merge('database' => 'IMPORT_DB'))

    db_scripts = create_dir("databases")
    module_name = 'MyModule'
    table_names = ['[MyModule].[foo]']
    database = create_simple_db_definition(db_scripts, module_name, table_names)
    database.separate_import_task = true
    import = database.add_import(:default, {})

    Dbt::Config.default_pre_import_dirs = ['a','b']
    pre_import_sql = "SELECT 1"
    create_file("databases/a/yyy.sql", pre_import_sql)
    pre_import_sql_2 = "SELECT 2"
    create_file("databases/b/qqq.sql", pre_import_sql_2)

    Dbt::Config.default_post_import_dirs = ['c','d']
    post_import_sql = "SELECT 3"
    create_file("databases/c/xxx.sql", post_import_sql)
    post_import_sql_2 = "SELECT 4"
    create_file("databases/d/zzz.sql", post_import_sql_2)

    mock.expects(:open).with(config, false).in_sequence(@s)
    Dbt.runtime.expects(:info).with("               : a/yyy.sql").in_sequence(@s)
    mock.expects(:execute).with(pre_import_sql, true).in_sequence(@s)
    Dbt.runtime.expects(:info).with("               : b/qqq.sql").in_sequence(@s)
    mock.expects(:execute).with(pre_import_sql_2, true).in_sequence(@s)
    expect_delete_for_table_import(mock, module_name, 'foo')
    expect_default_table_import(mock, import, module_name, 'foo')
    mock.expects(:post_data_module_import).with(import, module_name).in_sequence(@s)
    Dbt.runtime.expects(:info).with("               : c/xxx.sql").in_sequence(@s)
    mock.expects(:execute).with(post_import_sql, true).in_sequence(@s)
    Dbt.runtime.expects(:info).with("               : d/zzz.sql").in_sequence(@s)
    mock.expects(:execute).with(post_import_sql_2, true).in_sequence(@s)
    mock.expects(:post_database_import).with(import).in_sequence(@s)
    mock.expects(:close).with()

    Dbt.runtime.database_import(database.import_by_name(:default), nil)
  end

  # TODO: test import with module group
  # TODO: test migrations
  # TODO: test migrate where existing migrations exist
  # TODO: test post create migrations setup
  # TODO: test post create migrations setup with assume_migrations_applied_at_create?
  # TODO: test load_datasets_for_modules
  # TODO: test up module group
  # TODO: test down module group
  # TODO: test dump_tables_to_fixtures
  # TODO: test index files changing the order
  # TODO: test filters ??

  def setup
    super
    @s = sequence('main')
  end

  def create_table_sql(dir, table_name)
    create_file("databases/#{dir}/#{table_name}.sql", "CREATE TABLE [#{table_name}]")
  end

  def create_fixture(module_name, table_name)
    create_file("databases/#{module_name}/#{Dbt::Config.fixture_dir_name}/#{module_name}.#{table_name}.yml", "1:\n  ID: 1\n")
  end

  def expect_create_table(mock, module_name, dirname, table_name, seq = true)
    Dbt.runtime.expects(:info).with("#{'%-15s' % module_name}: #{dirname}#{table_name}.sql").in_sequence(@s)
    mock.expects(:execute).with("CREATE TABLE [#{table_name}]", false).in_sequence(@s)
  end

  def expect_delete(mock, module_name, table_name)
    mock.expects(:execute).with("DELETE FROM [#{module_name}].[#{table_name}]", false).in_sequence(@s)
  end

  def expect_fixture(mock, module_name, table_name)
    Dbt.runtime.expects(:info).with("Fixture        : #{module_name}.#{table_name}").in_sequence(@s)
    mock.expects(:pre_fixture_import).with("[#{module_name}].[#{table_name}]").in_sequence(@s)
    mock.expects(:insert).with("[#{module_name}].[#{table_name}]", 'ID' => 1).in_sequence(@s)
    mock.expects(:post_fixture_import).with("[#{module_name}].[#{table_name}]").in_sequence(@s)
  end

  def expect_default_table_import(mock, import_definition, module_name, table_name)
    expect_pre_table_import(mock, import_definition, module_name, table_name, 'D')
    mock.expects(:column_names_for_table).with("[#{module_name}].[#{table_name}]").returns(['[ID]']).in_sequence(@s)
    mock.expects(:execute).with("INSERT INTO DBT_TEST.[#{module_name}].[#{table_name}]([ID])\n  SELECT [ID] FROM IMPORT_DB.[#{module_name}].[#{table_name}]\n", true).in_sequence(@s)
    expect_post_table_import(mock, import_definition, module_name, table_name)
  end

  def expect_pre_table_import(mock, import_definition, module_name, table_name, import_type)
    mock.expects(:pre_table_import).with(import_definition, "[#{module_name}].[#{table_name}]").in_sequence(@s)
    Dbt.runtime.expects(:info).with("#{'%-15s' % module_name}: Importing #{module_name}.#{table_name} (By #{import_type})").in_sequence(@s)
  end

  def expect_post_table_import(mock, import_definition, module_name, table_name)
    mock.expects(:post_table_import).with(import_definition, "[#{module_name}].[#{table_name}]").in_sequence(@s)
  end

  def expect_delete_for_table_import(mock, module_name, table_name)
    Dbt.runtime.expects(:info).with("Deleting #{module_name}.#{table_name}").in_sequence(@s)
    expect_delete(mock, module_name, table_name)
  end

  def create_simple_db_definition(db_scripts, module_name, table_names)
    Dbt.add_database(:default) do |db|
      db.rake_integration = false
      db.modules = [module_name]
      db.table_map = {module_name => table_names}
      db.search_dirs = [db_scripts]
    end
  end

  def create_postgres_config(config = {}, top_level_config = {})
    Dbt::Config.driver = 'Pg'
    Dbt.repository.configuration_data = {
      Dbt::Config.environment => base_postgres_config(config)
    }.merge(top_level_config)
    Dbt.repository.configuration_for_key(Dbt::Config.environment)
  end

  def base_postgres_config(config = {})
    {
      'database' => 'DBT_TEST',
      'username' => ENV['USER'],
      'password' => 'letmein',
      'host' => '127.0.0.1',
      'port' => 5432
    }.merge(config)
  end
end
