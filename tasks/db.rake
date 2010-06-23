require File.expand_path(File.dirname(__FILE__) + '/../lib/db_tasks')

def check_db_env
  raise "DB_ENV not specified." if DB_ENV.nil?
end

namespace :dbt do
  task :load_config => ['clear'] do
    filename = "#{BASE_APP_DIR}/config/database.yml"
    ActiveRecord::Base.configurations = YAML::load(ERB.new(IO.read(filename)).result)
  end

  desc "Generate SQL for all databases."
  task :pre_build => 'dbt:load_config' do
  end
end

