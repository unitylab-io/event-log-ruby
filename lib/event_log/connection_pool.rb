module EventLog
  class ConnectionPool
    attr_reader :pool_size

    def initialize(options = {})
      @pool_size = options.fetch(:pool_size, 4).to_i
      @connections = Queue.new.tap do |queue|
        @pool_size.times.collect do
          queue << Aws::DynamoDB::Client.new(options.fetch(:client_config, {}))
        end
      end
    end

    def with_connection(&_block)
      conn = @connections.pop

      begin
        return yield(conn)
      ensure
        @connections << conn
      end
    end
  end
end
