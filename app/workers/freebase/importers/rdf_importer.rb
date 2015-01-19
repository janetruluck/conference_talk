# app/workers/freebase/importer/rdf_importer.rb
module Freebase
  module Importer
    class Rdf
      include Sidekiq::Worker
      include DataValidation

      PARSE_RULES = {
        "description" => "capture",
        "genre" => "crawl"
      }

      sidekiq_options :queue => :freebase_importer_rdf

      def perform(data_file)
        @processed_data = Hash.new
        process_and_import(data_file)
      end

      private
      # Massage the data to be in the format we want
      def process_and_import(data_file)
        items = File.open(data_file)

        items.each_line do |item|
          PARSE_RULES.keys.each do |key|
            if item.match(key)
              process_line(item, PARSE_RULKES.fetch(key))
            end
          end

          film = Film.new(@processed_data)
          film.save
        end
      end

      def process_line(line, rule, key)
        case rule
        when "capture"
          @processed_data[key] = line.match(/".*"/)
        when "crawl"
          RdfCrawler.perform_async("m.#{line.split("m.")}")
        end
      end
    end
  end
end
