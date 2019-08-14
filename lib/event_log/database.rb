module EventLog
  class Database
    attr_accessor :table_name, :index_table_name
    attr_reader   :namespace, :options

    def initialize(namespace, table_prefix = 'eventlog', options = {})
      @options = options.dup.freeze
      @namespace = namespace.to_s
      @table_name = "#{table_prefix}-events".to_s
      @index_table_name = options.fetch(:index_table_name, "#{table_name}-idx")
      @query_executor = ::EventLog::QueryExecutor.new(options)
      @event_fetcher_pool = EventFetcher.new(
        self, options.fetch(:event_fetcher_pool_size, 12)
      )
    end

    def list_event_types
      @query_executor.find_all(
        table_name: index_table_name,
        key_condition_expression: '#n = :n',
        expression_attribute_names: { '#n' => 'n' },
        expression_attribute_values: { ':n' => "#{namespace},event_types" }
      ).map do |item|
        item['v']
      end
    end

    def find_events(from, to = Time.now, options = {})
      partitions = find_partitions(from, to)
      partitions.flat_map do |partition|
        find_events_from_partition(partition, from, to, options)
      end.sort_by(&:date)
    end

    def find_events_by_type(type, from, to = Time.now, options = {})
      partitions = find_partitions_by_event(type, from, to)
      partitions.flat_map do |partition|
        find_events_from_partition(partition, from, to, options)
      end.sort_by(&:date)
    end

    def find_partitions(from, to = Time.now)
      query_params = {
        table_name: index_table_name,
        consistent_read: false,
        key_condition_expression: '#n = :n AND #v BETWEEN :from AND :to',
        expression_attribute_values: {
          ':n' => "#{namespace},partitions",
          ':from' => from.to_i.to_s, ':to' => to.to_i.to_s
        },
        expression_attribute_names: { '#n' => 'n', '#v' => 'v' }
      }
      @query_executor.find_all(query_params).map do |item|
        ::EventLog::TimePartition.new(item['n'], item['v'])
      end
    end

    def find_partitions_by_event(event, from, to = Time.now)
      query_params = {
        table_name: index_table_name,
        consistent_read: false,
        key_condition_expression: '#n = :n AND #v BETWEEN :from AND :to',
        expression_attribute_values: {
          ':n' => "#{namespace},partitions,#{event}",
          ':from' => from.to_i.to_s, ':to' => to.to_i.to_s
        },
        expression_attribute_names: { '#n' => 'n', '#v' => 'v' }
      }
      @query_executor.find_all(query_params).map do |item|
        ::EventLog::TimePartition.new(item['n'], item['v'])
      end
    end

    def find(uuid)
      record = @query_executor.find_one(table_name, uuid: uuid)
      ::EventLog::Event.from_record(record)
    end

    def publish(type, data = nil, date = Time.now)
      event = ::EventLog::Event.new(type: type, data: data, date: date)
      batch_write_request_items = {
        table_name => [
          { put_request: { item: event.as_record } },
        ],
        index_table_name => [
          {
            put_request: {
              item: {
                n: "#{namespace},partitions", v: event.time_partition.to_s
              }
            }
          },
          {
            put_request: {
              item: {
                n: "#{namespace},partitions,#{event.type}",
                v: event.time_partition.to_s
              }
            }
          },
          {
            put_request: {
              item: {
                n: "#{namespace},partitions,#{event.time_partition}",
                v: "#{event.timestamp},#{event.type},#{event.checksum}",
                e: event.uuid
              }
            }
          },
          {
            put_request: {
              item: {
                n: "#{namespace},partitions,#{event.type},#{event.time_partition}",
                v: "#{event.timestamp},#{event.checksum}",
                e: event.uuid
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
      loop do
        resp = build_dynamodb_client.batch_write_item(
          request_items: batch_write_request_items
        )
        break if resp.unprocessed_items.empty?
      end
      event
    end

    def build_dynamodb_client
      Aws::DynamoDB::Client.new(options.fetch(:dynamodb_config, {}))
    end

    private

    def find_events_from_partition(partition, from, to, options = {})
      criteria = {
        table_name: index_table_name,
        consistent_read: false,
        key_condition_expression: '#n = :n AND #v BETWEEN :from AND :to',
        expression_attribute_names: { '#n' => 'n', '#v' => 'v' },
        expression_attribute_values: {
          ':n' => "#{partition.name},#{partition.timestamp}",
          ':from' => "#{from.to_i * 1000},",
          ':to' => "#{to.to_i * 1000},"
        },
        projection_expression: 'e',
        scan_index_forward: \
          options.fetch(:order, :asc).to_sym == :desc ? false : true
      }

      @query_executor.find_all(criteria).each_slice(100).flat_map do |items|
        @event_fetcher_pool.batch_get_events(
          items.collect { |item| item['e'] }
        )
      end
    end
  end
end
