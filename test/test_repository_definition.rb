require File.expand_path('../helper', __FILE__)

class TestRepositoryDefinition < Dbt::TestCase

  def test_defaults
    definition = Dbt::RepositoryDefinition.new
    assert_equal definition.schema_overrides, {}
    assert_equal definition.table_map, {}
    assert_equal definition.modules, []
    assert_equal definition.to_yaml, "---\nmodules: !omap []\n"
  end

  def test_table_ordering
    definition = Dbt::RepositoryDefinition.new
    definition.table_map = {'mySchema' => %w(tblOne tblTwo)}
    assert_equal definition.table_ordering('mySchema'), %w(tblOne tblTwo)
    assert_equal definition.table_ordering?('mySchema'), true
    assert_equal definition.table_ordering?('NoSchemaHere'), false
    assert_raises(RuntimeError) do
      definition.table_ordering('NoSchemaHere')
    end
  end

  def test_sequence_ordering
    definition = Dbt::RepositoryDefinition.new
    definition.sequence_map = {'mySchema' => %w(tblOneSeq tblTwoSeq)}
    assert_equal definition.sequence_ordering('mySchema'), %w(tblOneSeq tblTwoSeq)
    assert_equal definition.sequence_ordering?('mySchema'), true
    assert_equal definition.sequence_ordering?('NoSchemaHere'), false
    assert_raises(RuntimeError) do
      definition.sequence_ordering('NoSchemaHere')
    end
  end

  def test_ordered_elements_for_module
    definition = Dbt::RepositoryDefinition.new
    definition.table_map = {'mySchema' => %w(tblOne tblTwo)}
    definition.sequence_map = {'mySchema' => %w(tblOneSeq tblTwoSeq)}
    assert_equal definition.ordered_elements_for_module('mySchema'), %w(tblOne tblTwo tblOneSeq tblTwoSeq)
  end

  def test_merge
    definition = Dbt::RepositoryDefinition.new
    definition.schema_overrides = {'Core' => 'C'}
    definition.table_map = {'Core' => '"C"."tblA"'}
    definition.modules = ['Core']

    definition2 = Dbt::RepositoryDefinition.new
    definition2.schema_overrides = {'Other' => 'O'}
    definition2.table_map = {'Other' => '"O"."tblB"'}
    definition2.modules = ['Other']

    merged = false
    begin
      definition.merge!(definition)
      merged = true
    rescue
      # Ignored
    end
    raise 'Succeeded in merging self - error!' if merged

    definition.merge!(definition2)

    assert_equal definition.schema_overrides, {'Core' => 'C','Other' => 'O'}
    assert_equal definition.table_map, {'Core' => '"C"."tblA"', 'Other' => '"O"."tblB"'}
    assert_equal definition.modules, %w(Core Other)
  end

  def test_from_yaml
    definition = Dbt::RepositoryDefinition.new
    definition.from_yaml(<<-CONFIG)
---
modules: !omap
- CodeMetrics:
    schema: CodeMetrics
    tables:
    - '[CodeMetrics].[tblCollection]'
    - '[CodeMetrics].[tblMethodMetric]'
    sequences:
    - '[CodeMetrics].[tblCollection_IDSeq]'
- Geo:
    schema: Geo
    tables:
    - '[Geo].[tblMobilePOI]'
    - '[Geo].[tblPOITrack]'
    - '[Geo].[tblSector]'
    - '[Geo].[tblOtherGeom]'
    sequences:
    - '[Geo].[tblMobilePOIIDSeq]'
    - '[Geo].[tblPOITrackIDSeq]'
    - '[Geo].[tblSectorIDSeq]'
- TestModule:
    schema: TM
    tables:
    - '[TM].[tblBaseX]'
    - '[TM].[tblFoo]'
    - '[TM].[tblBar]'
    sequences: []
    CONFIG

    assert_equal definition.modules, %w(CodeMetrics Geo TestModule)
    assert_equal definition.schema_overrides.size, 1
    assert_equal definition.schema_name_for_module('TestModule'), 'TM'
    assert_equal definition.table_ordering('Geo'), %w([Geo].[tblMobilePOI] [Geo].[tblPOITrack] [Geo].[tblSector] [Geo].[tblOtherGeom])
    assert_equal definition.sequence_ordering('CodeMetrics'), %w([CodeMetrics].[tblCollection_IDSeq])
    assert_equal definition.sequence_ordering('Geo'), %w([Geo].[tblMobilePOIIDSeq] [Geo].[tblPOITrackIDSeq] [Geo].[tblSectorIDSeq])
    assert_equal definition.sequence_ordering('TestModule'), []
  end

  def test_schema_name_for_module
    definition = Dbt::RepositoryDefinition.new
    definition.schema_overrides = {'bar' => 'baz'}
    definition.modules = ['foo','bar']
    assert_equal definition.schema_name_for_module('foo'), 'foo'
    assert_equal definition.schema_name_for_module('bar'), 'baz'
  end

  def test_modules
    definition = Dbt::RepositoryDefinition.new
    modules = ['a', 'b']
    definition.modules = modules
    assert_equal definition.modules, modules

    modules = ['a', 'b', 'c']
    definition.modules = Proc.new { modules }
    assert_equal definition.modules, modules
  end

  def test_to_yaml
    definition = Dbt::RepositoryDefinition.new
    definition.modules = ['CodeMetrics', 'Geo', 'TestModule']
    definition.schema_overrides = {'TestModule' => 'TM'}
    definition.table_map = {
      'TestModule' => %w([TM].[tblBaseX] [TM].[tblFoo] [TM].[tblBar]),
      'Geo' => %w([Geo].[tblMobilePOI] [Geo].[tblPOITrack] [Geo].[tblSector] [Geo].[tblOtherGeom]),
      'CodeMetrics' => %w([CodeMetrics].[tblCollection] [CodeMetrics].[tblMethodMetric])
    }
    assert_equal definition.to_yaml, <<YAML
---
modules: !omap
   - CodeMetrics:
      schema: CodeMetrics
      tables:
        - "[CodeMetrics].[tblCollection]"
        - "[CodeMetrics].[tblMethodMetric]"
      sequences: []
   - Geo:
      schema: Geo
      tables:
        - "[Geo].[tblMobilePOI]"
        - "[Geo].[tblPOITrack]"
        - "[Geo].[tblSector]"
        - "[Geo].[tblOtherGeom]"
      sequences: []
   - TestModule:
      schema: TM
      tables:
        - "[TM].[tblBaseX]"
        - "[TM].[tblFoo]"
        - "[TM].[tblBar]"
      sequences: []
YAML
  end

end
