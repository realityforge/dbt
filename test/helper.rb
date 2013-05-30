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
  end

  def teardown
  end
end
