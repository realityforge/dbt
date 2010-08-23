require 'db_tasks'

namespace :dbt do
  task :environment do
    require 'activerecord'
    require 'active_record/fixtures'
    require(File.join(RAILS_ROOT, 'config', 'environment'))
  end

  task :load_config => ['dbt:environment'] do
    require 'activerecord'
    require 'active_record/fixtures'
    # TODO: Fix this so it runs a hook here that loads database
    # rather than invoking dbt:environment as precondition
    ActiveRecord::Base.configurations = YAML::load(ERB.new(IO.read(DbTasks::Config.config_filename)).result)
  end
end
