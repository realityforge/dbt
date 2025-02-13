#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

class Dbt #nodoc

  DatabaseNameFilter = ::Struct.new('DatabaseNameFilter', :pattern, :database_key, :optional)
  PropertyFilter = ::Struct.new('PropertyFilter', :pattern, :value)
  DynamicPropertyFilter = ::Struct.new('DynamicPropertyFilter', :pattern, :value_key, :default_value)

  # Container class that is mixed into classes responsible for filtering sql files
  module FilterContainer
    def add_filter(&block)
      self.filters << block
    end

    def add_database_name_filter(pattern, database_key, optional = false)
      self.filters << DatabaseNameFilter.new(pattern, database_key, optional)
    end

    # Filter the SQL files replacing the specified pattern with the specified value
    def add_property_filter(pattern, value)
      self.filters << PropertyFilter.new(pattern, value)
    end

    # Filter that replaces the specified pattern with a value from the dynamic property store or the default value if none-specified. 
    def add_dynamic_property_filter(pattern, value_key, default_value = '')
      self.filters << DynamicPropertyFilter.new(pattern, value_key, default_value)
    end

    # Makes the import scripts support statements such as
    #   ASSERT_ROW_COUNT(1)
    #   ASSERT_ROW_COUNT(SELECT COUNT(*) FROM Foo)
    #   ASSERT_UNCHANGED_ROW_COUNT()
    #   ASSERT(@Id IS NULL)
    #
    attr_writer :import_assert_filters

    def import_assert_filters?
      return @import_assert_filters unless @import_assert_filters.nil?
      !self.respond_to?(:imports) || self.imports.size > 0
    end

    attr_writer :database_environment_filter

    def database_environment_filter?
      @database_environment_filter.nil? ? false : @database_environment_filter
    end

    def filters
      @filters ||= []
    end

    def expanded_filters(environment = Dbt::Config.environment)
      filters = []
      if import_assert_filters?
        filters << Proc.new do |sql|
          sql = sql.gsub(/ASSERT_DATABASE_VERSION\((.*)\)/, <<SQL)
GO
BEGIN
  DECLARE @DbVersion VARCHAR(MAX)
  SET @DbVersion = ''
  SELECT @DbVersion = COALESCE(CONVERT(VARCHAR(MAX),value),'')
    FROM [__SOURCE__].sys.fn_listextendedproperty('DatabaseSchemaVersion', default, default, default, default, default, default)
  IF (@DbVersion IS NULL OR @DbVersion = '\\1')
  BEGIN
    DECLARE @Message VARCHAR(MAX)
    SET @Message = CONCAT('Expected DatabaseSchemaVersion in __SOURCE__ database not to be \\1. Actual Value: ', @DbVersion)
    RAISERROR (@Message, 16, 1) WITH SETERROR
  END
END
GO
BEGIN
  DECLARE @DbVersion VARCHAR(MAX)
  SET @DbVersion = ''
  SELECT @DbVersion = COALESCE(CONVERT(VARCHAR(MAX),value),'')
    FROM [__TARGET__].sys.fn_listextendedproperty('DatabaseSchemaVersion', default, default, default, default, default, default)
  IF (@DbVersion IS NULL OR @DbVersion != '\\1')
  BEGIN
    DECLARE @Message VARCHAR(MAX)
    SET @Message = CONCAT('Expected DatabaseSchemaVersion in __TARGET__ database to be \\1. Actual Value: ', @DbVersion)
    RAISERROR (@Message, 16, 1) WITH SETERROR
  END
END
GO
SQL
          sql = sql.gsub(/ASSERT_UNCHANGED_ROW_COUNT\(\)/, <<SQL)
GO
IF (SELECT COUNT(*) FROM [__TARGET__].@@TABLE@@) != (SELECT COUNT(*) FROM [__SOURCE__].@@TABLE@@)
BEGIN
  RAISERROR ('Actual row count for @@TABLE@@ does not match expected rowcount', 16, 1) WITH SETERROR
END
SQL
          sql = sql.gsub(/ASSERT_ROW_COUNT\((.*)\)/, <<SQL)
GO
IF (SELECT COUNT(*) FROM [__TARGET__].@@TABLE@@) != (\\1)
BEGIN
  RAISERROR ('Actual row count for @@TABLE@@ does not match expected rowcount', 16, 1) WITH SETERROR
END
SQL
          sql = sql.gsub(/ASSERT\((.+)\)/, <<SQL)
GO
IF NOT (\\1)
BEGIN
  RAISERROR ('Failed to assert \\1', 16, 1) WITH SETERROR
END
SQL
          sql
        end
      end

      if database_environment_filter?
        filters << Proc.new do |sql|
          sql.gsub(/@@ENVIRONMENT@@/, environment.to_s).gsub(/__ENVIRONMENT__/, environment.to_s)
        end
      end

      self.filters.each do |filter|
        if filter.is_a?(PropertyFilter)
          filters << Proc.new do |sql|
            sql.gsub(filter.pattern, filter.value)
          end
        elsif filter.is_a?(DatabaseNameFilter)
          filters << Proc.new do |sql|
            Dbt.runtime.filter_database_name(sql, filter.pattern, filter.database_key, environment, filter.optional)
          end
        elsif filter.is_a?(DynamicPropertyFilter)
          filters << Proc.new do |sql|
            value = Dbt::Config.get_dynamic_property(filter.value_key)
            sql.gsub(/#{filter.pattern}/, value.nil? ? filter.default_value : value)
          end
        else
          filters << filter
        end
      end

      filters
    end
  end
end
