module EventLog
  class EventFetcher
    def initialize(db, pool_size = 4)
      @db = db
      @thread_pool = Concurrent::FixedThreadPool.new(pool_size)
    end

    def batch_get_events(uuid_arr)
      uuid_chunks = uuid_arr.each_slice(100).to_a
      events = Concurrent::Array.new
      countdown = Concurrent::CountDownLatch.new(uuid_chunks.length)
      uuid_chunks.each do |uuid_chunk|
        @thread_pool.post(uuid_chunk) do |chunk|
          keys = chunk.collect { |uuid| { uuid: uuid } }
          begin
            events.concat(
              safe_batch_get(keys).map do |record|
                ::EventLog::Event.from_record(record)
              end
            )
          rescue => ex
            raise ex
          ensure
            countdown.count_down
          end
        end
      end
      countdown.wait
      events
    end

    private

    def safe_batch_get(keys)
      client = @db.build_dynamodb_client
      resp = client.batch_get_item(
        request_items: {
          @db.table_name => { keys: keys }
        }
      )
      resp.responses[@db.table_name].tap do |events|
        next if resp.unprocessed_keys.empty?

        events.concat(safe_batch_get(resp.unprocessed_keys[@db.table_name]))
      end
    end
  end
end
