module EventLog
  class EventFetcher
    def initialize(events_table_name, connection_pool)
      @events_table_name = events_table_name
      @connection_pool = connection_pool
      @thread_pool = Concurrent::FixedThreadPool.new(connection_pool.pool_size)
    end

    def batch_get_events(uuids)
      uuid_chunks = uuids.each_slice(100).to_a
      uuid_chunks.flat_map do |uuid_chunk|
        keys = uuid_chunk.collect { |uuid| { uuid: uuid } }
        safe_batch_get(keys).map do |record|
          ::EventLog::Event.from_record(record)
        end
      end.sort_by(&:date)
    end

    private

    def safe_batch_get(keys)
      records = []
      loop do
        resp = @connection_pool.with_connection do |conn|
          conn.batch_get_item(
            request_items: { @events_table_name => { keys: keys } }
          )
        end
        records.concat(resp.responses[@events_table_name])
        if resp.unprocessed_keys[@events_table_name].nil? ||
           resp.unprocessed_keys[@events_table_name].empty?
          break
        end

        keys = resp.unprocessed_keys[@events_table_name]
      end
      records
    end
  end
end
