require File.expand_path('../helper', __FILE__)

class TestRuntimePackage < Dbt::TestCase

  def test_simple_package
    db_scripts = create_dir("databases/generated")
    create_file("databases/generated/MyModule/base.sql", "")
    create_file("databases/generated/MyModule/types/typeA.sql", "")
    create_file("databases/generated/MyModule/views/viewA.sql", "")
    create_file("databases/generated/MyModule/functions/functionA.sql", "")
    create_file("databases/generated/MyModule/stored-procedures/spA.sql", "")
    create_file("databases/generated/MyModule/misc/spA.sql", "")
    create_file("databases/generated/MyModule/fixtures/foo.yml", "")
    create_file("databases/generated/MyModule/fixtures/bar.sql", "")
    create_file("databases/generated/MyModule/triggers/trgA.sql", "")
    create_file("databases/generated/MyModule/finalize/finA.sql", "")
    create_file("databases/generated/MyModule/down/downA.sql", "")

    database = Dbt.add_database(:default) do |db|
      db.rake_integration = false
      db.modules = ['MyModule']
      db.table_map = {'MyModule' => ['foo','bar','baz']}
      db.search_dirs = [db_scripts]
    end

    output_dir = create_dir("pkg/out")
    Dbt.runtime.package_database_data(database, output_dir)

    assert_file_exist("#{output_dir}/MyModule/base.sql")
    assert_file_exist("#{output_dir}/MyModule/types/typeA.sql")
    assert_file_exist("#{output_dir}/MyModule/views/viewA.sql")
    assert_file_exist("#{output_dir}/MyModule/functions/functionA.sql")
    assert_file_exist("#{output_dir}/MyModule/stored-procedures/spA.sql")
    assert_file_exist("#{output_dir}/MyModule/misc/spA.sql")
    assert_file_exist("#{output_dir}/MyModule/fixtures/foo.yml")
    assert_file_exist("#{output_dir}/MyModule/fixtures/bar.sql")
    assert_file_exist("#{output_dir}/MyModule/triggers/trgA.sql")
    assert_file_exist("#{output_dir}/MyModule/finalize/finA.sql")
    assert_file_exist("#{output_dir}/MyModule/down/downA.sql")
  end

  def test_multiple_modules
    db_scripts = create_dir("databases/generated")
    create_file("databases/generated/MyModule/base.sql", "")
    create_file("databases/generated/MyOtherModule/base.sql", "")

    database = Dbt.add_database(:default) do |db|
      db.rake_integration = false
      db.modules = ['MyModule','MyOtherModule']
      db.table_map = {'MyModule' => [], 'MyOtherModule' => []}
      db.search_dirs = [db_scripts]
    end

    output_dir = create_dir("pkg/out")
    Dbt.runtime.package_database_data(database, output_dir)

    assert_file_exist("#{output_dir}/MyModule/base.sql")
    assert_file_exist("#{output_dir}/MyOtherModule/base.sql")
  end

end
