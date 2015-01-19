# app/workers/freebase/importer/film.rb
module Freebase
  module Importer
    class Film
      include Sidekiq::Worker
      include DataValidation

      sidekiq_options :queue => :freebase_importer_film

      def perform(data_file)
        process_and_import(data_file)
      end

      private
      # Massage the data to be in the format we want
      def process_and_import(data_file)
        items = JSON.load(File.open(data_file))["result"]

        items.each do |item|
          date = item["/film/film/initial_release_date"]

          next unless date

          case date.length
          when 4
            date = date + "-01-01"
          when 7
            date = date + "-01"
          end

          date = Date.parse(date)

          # Query current database so that we backfill/update information if the
          # film already exists
          film = Film.where(
            freebase_id: item["/type/object/mid"]
          ).first
          film = Film.where(
            title: item["/type/object/name"],
            year: date.year
          ).first unless film

          film               = Film.new unless film
          film.title         = valid_item?(item["/type/object/name"]) unless film.title
          film.release_date  = valid_item?(date) unless film.release_date
          film.year          = date.year unless film.year
          film.freebase_id   = item["/type/object/mid"] unless film.freebase_id
          film.freebase_guid = item["/type/object/guid"] unless film.freebase_guid
          film.save
        end
      end
    end
  end
end
