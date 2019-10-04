module EventLog
  class QueryExecutor
    def initialize(connection_pool)
      @connection_pool = connection_pool
    end

    def find_one(table_name, item_key)
      @connection_pool.with_connection do |conn|
        conn.get_item(table_name: table_name, key: item_key).item
      end
    end

    def count(criteria)
      criteria = criteria.merge(select: 'COUNT')
      total = 0
      loop do
        resp = @connection_pool.with_connection { |conn| conn.query(criteria) }
        total += resp.count
        break if resp.last_evaluated_key.nil?

        criteria[:exclusive_start_key] = resp.last_evaluated_key
      end
      total
    end

    def find_all(criteria)
      Enumerator.new do |arr|
        loop do
          resp = @connection_pool.with_connection { |conn| conn.query(criteria) }
          arr << resp.items
          break if resp.last_evaluated_key.nil?

          criteria[:exclusive_start_key] = resp.last_evaluated_key
        end
      end
    end
  end
end
