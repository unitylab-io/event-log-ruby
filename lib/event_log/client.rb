module EventLog
  class Client
    attr_accessor :table_name, :index_table_name
    attr_reader   :namespace

    def initialize(namespace, table_name, options = {})
      @namespace = namespace.to_s
      @table_name = table_name.to_s
      @index_table_name = options.fetch(:index_table_name, "#{table_name}-idx")
      @dynamodb_client = options.fetch(:dynamodb_client) do
        Aws::DynamoDB::Client.new
      end
    end

    def find_events(event, from, to = Time.now, options = {})
      partitions = find_partitions_by_event(event, from, to)
      partitions.flat_map do |partition|
        find_all_from_partition(partition, from, to, options)
      end
    end

    def find_partitions_by_event(event, from, to = Time.now)
      query_builder = ::EventLog::TimePartitionQueryBuilder.new(namespace)
      query_params = query_builder.as_query(event, from, to).merge(
        table_name: index_table_name, consistent_read: false
      )
      [].tap do |partitions|
        loop do
          resp = @dynamodb_client.query(query_params)
          partitions.concat(
            resp.items.map do |item|
              ::EventLog::TimePartition.from_record(item['r'])
            end
          )
          break if resp.last_evaluated_key.nil?

          query_params[:exclusive_start_key] = resp.last_evaluated_key
        end
      end
    end

    def find(event_id)
      indexed_record = @dynamodb_client.get_item(
        table_name: index_table_name,
        key: { n: "#{namespace},events", v: event_id }
      )
      record = @dynamodb_client.get_item(
        table_name: table_name, key: indexed_record.item['r']
      )
      ::EventLog::Event.from_record(partition, record.item)
    end

    def publish(*args)
      event = \
        if args.length == 1
          raise ArgumentError, 'Event object expected' unless args[0].is_a?(EventLog::Event)

          args[0]
        elsif args.length == 2
          ::EventLog::Event.new(namespace, args[0], Time.now, args[1])
        end
      event.tap do
        time_partition = event.time_partition
        record = event.as_record
        @dynamodb_client.batch_write_item(
          request_items: {
            table_name => [
              { put_request: { item: record } }
            ],
            index_table_name => [
              {
                put_request: {
                  item: {
                    n: "#{namespace},events", v: event.id,
                    r: { rk: record['rk'], tid: record['tid'] }
                  }
                }
              },
              {
                put_request: {
                  item: {
                    n: "#{namespace},partitions,#{event.type}",
                    v: time_partition.timestamp.to_s,
                    r: {
                      ns: namespace,
                      date: time_partition.timestamp,
                      type: event.type
                    }
                  }
                }
              },
              {
                put_request: {
                  item: { n: "#{namespace},event_types", v: event.type }
                }
              }
            ]
          }
        )
      end
    end

    private

    def find_all_from_partition(partition, from, to, options = {})
      query_builder = EventQueryBuilder.new(partition)
      criteria = query_builder.as_query(from, to).merge(
        table_name: table_name, consistent_read: false,
        projection_expression: 'tid, ds',
        scan_index_forward: \
          options.fetch(:order, :asc).to_s == 'asc' ? true : false
      )
      [].tap do |events|
        loop do
          resp = @dynamodb_client.query(criteria)
          events.concat(
            resp.items.map do |item|
              ::EventLog::Event.from_record(partition, item)
            end
          )
          break if resp.last_evaluated_key.nil?

          criteria[:exclusive_start_key] = resp.last_evaluated_key
        end
      end
    end
  end
end
