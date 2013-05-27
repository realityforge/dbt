require File.expand_path('../helper', __FILE__)

class TestBasic < Dbt::TestCase
  def test_basic
    assert_equal "x", "x"
  end
end
