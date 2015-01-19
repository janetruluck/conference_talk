require 'cgi'
require 'httparty'
require 'json'
require 'addressable/uri'
require 'fileutils'

module Freebase
  class Crawler
    include Sidekiq::Worker
    include Freebase

    sidekiq_options :queue => :freebase_crawler

    attr_reader :type, :crawl_schema, :crawl_url, :crawl_query

    def perform(type="film", cursor = '')
      @type  = type
      cursor = crawl

      # Keep going while there is more data
      while cursor
        cursor = crawl(cursor)
      end

      # Split data in file
      Freebase::Splitter.perform_async(type)
    end

    private
    def crawl(cursor = "")
      query_values = {
        'query'  => crawl_query,
        'cursor' => cursor
      }

      query_values.merge!({
        'key'    => FREEBASE_API_KEY,
      }) if FREEBASE_API_KEY

      crawl_url.query_values = query_values

      response = HTTParty.get(crawl_url, :format => :json)

      # Save the current response to a file for later processing
      export_to_file(response) if response

      return response["cursor"]
    end

    def export_to_file(data)
      file_path = "#{data_dir}/#{SecureRandom.uuid}.json"

      FileUtils.mkdir_p(data_dir) unless Dir.exists?(data_dir)

      File.open(file_path,"a+") do |f|
        f.write(data.to_json)
      end
    end

    def crawl_schema
      @crawl_schema ||= "#{schema_dir}/freebase_query_#{type}.json"
    end

    def crawl_query
      @query ||= JSON.parse(IO.read(crawl_schema))
    end

    def crawl_url
      @crawl_url ||= Addressable::URI.parse(FREEBASE_URL)
    end
  end
end
