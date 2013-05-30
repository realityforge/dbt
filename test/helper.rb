$:.unshift File.expand_path('../../lib', __FILE__)

begin
  gem 'minitest'
rescue Gem::LoadError
end

require 'minitest/autorun'
require 'dbt'
require 'tmpdir'

class Dbt::TestCase < Minitest::Test
  def setup
    Dbt::Config.default_search_dirs = nil
    Dbt::Config.default_no_create = nil
    Dbt::Config.config_filename = nil
  end

  def teardown
  end
end
