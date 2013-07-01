require File.expand_path('../helper', __FILE__)

class TestDbConfig < Dbt::TestCase

  def test_tiny_tds_config
    run_sql_server_test(Dbt::TinyTdsDbConfig)
  end

  def test_mssql_config
    config = run_sql_server_test(Dbt::MssqlDbConfig)
    assert_equal config.jdbc_driver, 'net.sourceforge.jtds.jdbc.Driver'
    assert_equal config.jdbc_url(true), "jdbc:jtds:sqlserver://example.com:1433/msdb;instance=myinstance;appname=app-ick"
    assert_equal config.jdbc_url(false), "jdbc:jtds:sqlserver://example.com:1433/DB;instance=myinstance;appname=app-ick"
  end

  def run_sql_server_test(config_class)
    instance = 'myinstance'
    appname = 'app-ick'
    data_path = 'C:\\someDir'
    log_path = 'C:\\someDir'
    restore_from = 'C:\\someDir\\foo.bak'
    backup_location = 'C:\\someDir\\bar.bak'
    instance_registry_key = 'MSQL10.05'
    force_drop = true

    config = new_base_config.merge(
      :instance => instance,
      :appname => appname,
      :data_path => data_path,
      :log_path => log_path,
      :restore_from => restore_from,
      :backup_location => backup_location,
      :instance_registry_key => instance_registry_key,
      :force_drop => force_drop
    )
    config = config_class.new(config)
    assert_base_config(config, 1433)

    assert_equal config.instance, instance
    assert_equal config.appname, appname
    assert_equal config.data_path, data_path
    assert_equal config.log_path, log_path
    assert_equal config.restore_from, restore_from
    assert_equal config.backup_location, backup_location
    assert_equal config.instance_registry_key, instance_registry_key
    assert_equal config.force_drop?, force_drop

    config.force_drop = nil
    assert_equal config.force_drop?, false
    config
  end

  def assert_base_config(config, base_port)
    assert_equal config.database, 'DB'
    assert_equal config.username, 'god'
    assert_equal config.password, 'secret'
    assert_equal config.host, 'example.com'
    assert_equal config.port, 1234

    config.port = nil
    assert_equal config.port, base_port
  end

  def new_base_config
    {
      :database => 'DB',
      :username => 'god',
      :password => 'secret',
      :host => 'example.com',
      :port => 1234
    }
  end
end