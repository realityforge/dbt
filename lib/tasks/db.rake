require 'db_tasks'

namespace :dbt do
  task :load_config => ['clear'] do
    require 'activerecord'
    require 'active_record/fixtures'
    # TODO: Fix this so it runs a hook that actually loads database drivers
    require(File.join(RAILS_ROOT, 'config', 'environment'))
    ActiveRecord::Base.configurations = YAML::load(ERB.new(IO.read(DbTasks::Config.config_filename)).result)
  end
end
