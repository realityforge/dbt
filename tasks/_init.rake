def rm_tasks_like(re)
  Rake.application.instance_variable_get(:@tasks).delete_if { |x, y| x.to_s =~ re }
end

def rm_task(task)
  Rake.application.instance_variable_get(:@tasks).delete_if { |x, y| x.to_s == task }
end

# Remove all the "standard" db tasks as not appropriate for our model
rm_tasks_like(/^db:/)
# Remove all the "standard" doc tasks as we never use them
rm_tasks_like(/^doc:/)
# Remove the "rails" tasks we never use
rm_tasks_like(/^rails:/)
# Remove assorted tasks that are not used or not appropriate for our setup
rm_task('stats')
rm_task('secret')
rm_task('routes')
rm_tasks_like(/^notes:/)
rm_task('notes')
# Remove the "test" tasks we never use
rm_tasks_like(/^test:/)
rm_task('test')
rm_tasks_like(/^tmp:/)
rm_tasks_like(/^log:/)
rm_tasks_like(/^backups:/)
rm_tasks_like(/^gems/)
rm_tasks_like(/^time:/)
rm_task('clear')

desc "Cleanup temporary, log and backup files"
task :clear  do
  FileUtils.rm_rf(Dir["#{BASE_APP_DIR}/tmp/[^.]*"])
end
