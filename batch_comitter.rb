require 'rubygems'
require 'bunny'
require 'eventmachine'
require 'neography'

BATCH_SIZE = 500

class BatchProcessor
  def initialize
    @commands = []
    @tags = []

    conn = Bunny.new ENV['BROKER_URL']
    conn.start
    @ch = conn.create_channel
    @ch.prefetch(BATCH_SIZE)
    @queue = @ch.queue('batch-execute-neo4j', durable: true)

    @neo = Neography::Rest.new server: ENV['NEO4J_SERVER']

    @queue.subscribe(ack: true) do |delivery_info, properties, payload|
      begin
        @commands << [:execute_query, payload.gsub(/u = \(/, '(u').gsub(/t = \(/, '(t')]
        @tags << delivery_info.delivery_tag

        if @commands.size == BATCH_SIZE
          process_commands
          acknowledge_tags
        end
      rescue StandardError => error
        puts "Error: #{error.message}"
        reject_tags
      end
    end
  end

  private

    def process_commands
      time = Time.now
      print "[#{time}] Executing #{BATCH_SIZE} batch..."
      commands = @commands
      @commands = []
      @neo.batch *commands
      puts "Done (in #{Time.now - time} seconds)"
    end

    def acknowledge_tags
      @ch.acknowledge(@tags.last, true)
      @tags = []
    end

    def reject_tags
      @tags.each do |tag|
        @ch.reject(tag, true)
      end
      @tags = []
    end

end

# RUN RUN RUN

EM.run do
  BatchProcessor.new
end