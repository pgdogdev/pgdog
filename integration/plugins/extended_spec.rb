# frozen_string_literal: true

require 'pg'
require 'rspec'
require 'fileutils'

describe 'extended protocol' do
  let(:plugin_marker_file) { File.expand_path('../test-plugins/test-plugin-compatible/route-called.test', __FILE__) }

  before do
    # Delete the marker file before running tests
    puts "DEBUG: Looking for plugin marker file at: #{plugin_marker_file}"
    FileUtils.rm_f(plugin_marker_file)
    expect(File.exist?(plugin_marker_file)).to be false
  end

  it 'can pass params to plugin' do
    10.times do
      conn = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog')
      25.times do |i|
        result = conn.exec 'SELECT t.id FROM (SELECT $1 AS id) t WHERE t.id = $1', [i]
        expect(result[0]['id'].to_i).to eq(i)
      end
    end

    # Verify the plugin was actually called
    expect(File.exist?(plugin_marker_file)).to be true
  end
end
