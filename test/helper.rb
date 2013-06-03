$:.unshift File.expand_path('../../lib', __FILE__)

require 'minitest/autorun'
require 'dbt'
require 'tmpdir'

class Dbt::TestCase < Minitest::Test
  def setup
    Dbt::Config.default_search_dirs = nil
    Dbt::Config.default_no_create = nil
    Dbt::Config.config_filename = nil
    Dbt::Config.datasets_dir_name = nil
    Dbt::Config.repository_config_file = nil
    Dbt::Config.default_up_dirs = nil
    Dbt::Config.default_down_dirs = nil
    Dbt::Config.default_finalize_dirs = nil
    Dbt::Config.default_pre_create_dirs = nil
    Dbt::Config.default_post_create_dirs = nil
    Dbt::Config.default_pre_import_dirs = nil
    Dbt::Config.default_post_import_dirs = nil
    Dbt::Config.index_file_name = nil
    Dbt::Config.default_import = nil
    Dbt::Config.fixture_dir_name = nil
    Dbt::Config.environment = nil
    Dbt::Config.driver = nil
    Dbt::Config.default_migrations_dir_name = nil
    Dbt::Config.default_database = nil
    Dbt::Config.task_prefix = nil
  end

  def teardown
  end
end
