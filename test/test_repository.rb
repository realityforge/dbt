require File.expand_path('../helper', __FILE__)

class TestRepository < Dbt::TestCase
  def test_database_interactions
    repository = Dbt::Repository.new

    database = repository.add_database(:default)

    assert_equal repository.database_for_key(:default), database

    assert repository.database_keys.include?(:default)

    assert_raises(RuntimeError) do
      repository.add_database(:default)
    end

    assert_equal repository.database_for_key(:default), database

    assert repository.database_keys.include?(:default)

    assert_equal repository.remove_database(:default), database

    assert !repository.database_keys.include?(:default)

    assert_raises(RuntimeError) do
      repository.database_for_key(:default)
    end

    assert_raises(RuntimeError) do
      repository.remove_database(:default)
    end
  end

end
