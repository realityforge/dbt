class ActiveRecord::ConnectionAdapters::AbstractAdapter

  FK_ACTIONS = { :cascade => "CASCADE", :restrict => "RESTRICT", :set_null => "SET NULL", :set_default => "SET DEFAULT", :no_action => "NO ACTION" }.freeze

  def add_foreign_key(table_name, column_names, references_table_name, references_column_names, options = {})
    on_update = options[:on_update]
    on_delete = options[:on_delete]
    name = options[:name] || "FK_#{table_name}_#{Array(column_names).join("_")}"

    sql = "ALTER TABLE #{quote_table_name(table_name)} ADD CONSTRAINT #{name}"
    sql << " FOREIGN KEY (#{Array(column_names).join(", ")}) REFERENCES #{references_table_name} (#{Array(references_column_names).join(", ")})"
    sql << " ON UPDATE #{FK_ACTIONS[on_update]}" if on_update
    sql << " ON DELETE #{FK_ACTIONS[on_delete]}" if on_delete
    sql

    execute sql
  end

  # This is SQL server specific
  def create_schema(schema_name)
    execute "CREATE SCHEMA #{schema_name}"
  end
end
