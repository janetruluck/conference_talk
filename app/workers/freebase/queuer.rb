# app/workers/freebase/queuer.rb
module Freebase
  class Queuer
    include Sidekiq::Worker
    include Sidetiq::Schedulable
    include Freebase

    # Keep the worker going so recenlty added information is imported
    recurrence { daily(3) }
    sidekiq_options :queue => :freebase_queuer

    def perform
      # Queue crawler jobs for each Freebase "domain"
      # CRAWL_AREAS = [
      #   "cast",
      #   "film",
      #   ...
      # ]
      CRAWL_AREAS.each do |area|
        Freebase::Crawler.perform_async(area, '')
      end
    end
  end
end
