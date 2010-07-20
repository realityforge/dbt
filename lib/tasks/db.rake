require 'db_tasks'

namespace :dbt do
  task :load_config => ['clear'] do
    require 'activerecord'
    require 'active_record/fixtures'
    # TODO: Fix this so it runs a hook that actually loads database drivers
    task("dbt:environment").invoke
    ActiveRecord::Base.configurations = YAML::load(ERB.new(IO.read(DbTasks::Config.config_filename)).result)
  end

  desc "Generate SQL for all databases."
  task :pre_build => 'dbt:load_config' do
  end
end

