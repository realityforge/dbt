# The following section loads ActiveRecord if not already loaded
unless Object.const_defined? :ActiveRecord
  require 'erb'

  def add_to_paths(root_dir)
    Dir.glob( File.join(root_dir, "**/lib" )).each do|dir|
      $LOAD_PATH.insert(0, File.join(dir) )
    end
  end

  rails_root = defined?(::RAILS_FRAMEWORK_ROOT) ? ::RAILS_FRAMEWORK_ROOT : "#{RAILS_ROOT}/vendor/rails"
  require "#{rails_root}/activerecord/lib/activerecord.rb"

  gem_root = "#{BASE_APP_DIR}/vendor/gems"
  plugin_root = "#{BASE_APP_DIR}/vendor/plugins"
  shared_plugin_root = "#{BASE_APP_DIR}/shared/plugins"

  add_to_paths(gem_root)
  add_to_paths(plugin_root)
  add_to_paths(shared_plugin_root)
end
