module Freebase
  class ImportOrchestrator
    include Sidekiq::Worker
    include Freebase

    sidekiq_options :queue => :freebase_import_orchestrator

    def perform(type = nil)
      @type = type

      # queue the specified importer
      if @type
        import_data
      else # queue all importers
        CRAWL_AREAS.each do |type|
          @type = type

          import_data
        end
      end
    end

    private
    # Grab each previously gathered data file and
    # queue a worker for it to import the data
    def import_data
      data_files.each do |data_file|
        "Freebase::Importer::#{@type.camelize}".classify
        .constantize
        .perform_async(data_file)
      end
    end

    def data_files
      Dir["#{data_dir}/*"]
    end
  end
end
