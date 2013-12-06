require File.expand_path('../helper', __FILE__)
require 'tempfile'
require 'zip/zip'
require 'zip/zipfilesystem'

class TestCache < Dbt::TestCase

  def test_basic_workflow
    zip_filename = create_zip('data/File.txt' => 'myContents', 'ExcludedFile.txt' => 'blah')
    cache = Dbt::Cache.new
    assert_equal 0, cache.instance_variable_get("@package_cache").size
    cache.package(zip_filename)
    assert_equal 1, cache.instance_variable_get("@package_cache").size
    assert_equal 1, cache.package(zip_filename).files.size
    assert_equal 'myContents', cache.package(zip_filename).contents('File.txt')
    cache.reset
    assert_equal 0, cache.instance_variable_get("@package_cache").size
  end

  def create_zip(contents = {})
    tf = Tempfile.open('dbtpackage')
    zip_filename = tf.path
    tf.close
    Zip::ZipOutputStream.open(zip_filename) do |zip|
      contents.each_pair do |filename, file_content|
        zip.put_next_entry(filename)
        zip << file_content
      end
    end
    zip_filename
  end
end
