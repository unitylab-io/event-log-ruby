module EventLog
  class Database
    attr_accessor :table_name, :index_table_name
    attr_reader   :namespace, :options

    PUBLISH_OPERATIONS_BUILDER = %i[
      namespace_partition_operation_builder

    ].freeze

    def initialize(namespace, table_prefix = 'eventlog', options = {})
      @options = options.dup.freeze
      @namespace = namespace.to_s
      @table_name = "#{table_prefix}-events".to_s
      @index_table_name = options.fetch(:index_table_name, "#{table_name}-idx")
      @connection_pool = options.fetch(:connection_pool) do
        ::EventLog::ConnectionPool.new(options)
      end
      @query_executor = ::EventLog::QueryExecutor.new(@connection_pool)
      @event_fetcher_pool = EventFetcher.new(@table_name, @connection_pool)
    end

    def list_event_types
      @query_executor.find_all(
        table_name: index_table_name,
        key_condition_expression: '#n = :n',
        expression_attribute_names: { '#n' => 'n' },
        expression_attribute_values: { ':n' => "#{namespace},event_types" }
      ).flat_map do |item|
        item['v']
      end
    end

    def count_events(from, to = Time.now, options = {})
      partitions = find_partitions(from, to)
      partitions.sum do |partition|
        count_from_partition(partition, from, to, options)
      end
    end

    def count_events_by_type(event_type, from, to = Time.now, options = {})
      partitions = find_partitions_by_event(event_type, from, to)
      partitions.sum do |partition|
        count_from_partition(partition, from, to, options)
      end
    end

    def find_events(from, to = Time.now, options = {})
      partitions = find_partitions(from, to)
      Enumerator.new do |arr|
        partitions.each do |partition|
          find_events_from_partition(partition, from, to, options).each do |event|
            arr << event
          end
        end
        nil
      end
    end

    def find_events_by_type(type, from, to = Time.now, options = {})
      partitions = find_partitions_by_event(type, from, to)
      Enumerator.new do |arr|
        partitions.each do |partition|
          find_events_from_partition(partition, from, to, options).each do |event|
            arr << event
          end
        end
        nil
      end
    end

    def find_partitions(from, to = Time.now)
      query_params = {
        table_name: index_table_name,
        consistent_read: false,
        key_condition_expression: '#n = :n AND #v BETWEEN :from AND :to',
        expression_attribute_values: {
          ':n' => "#{namespace},partitions",
          ':from' => ::EventLog::TimePartition.timestamp_for(from.to_i).to_s,
          ':to' => to.to_i.to_s
        },
        expression_attribute_names: { '#n' => 'n', '#v' => 'v' }
      }
      @query_executor.find_all(query_params).flat_map do |items|
        items.map do |item|
          ::EventLog::TimePartition.new(item['n'], item['v'])
        end
      end
    end

    def find_partitions_by_event(event, from, to = Time.now)
      query_params = {
        table_name: index_table_name,
        consistent_read: false,
        key_condition_expression: '#n = :n AND #v BETWEEN :from AND :to',
        expression_attribute_values: {
          ':n' => "#{namespace},partitions,#{event}",
          ':from' => ::EventLog::TimePartition.timestamp_for(from.to_i).to_s,
          ':to' => to.to_i.to_s
        },
        expression_attribute_names: { '#n' => 'n', '#v' => 'v' }
      }
      @query_executor.find_all(query_params).flat_map do |items|
        items.map do |item|
          ::EventLog::TimePartition.new(item['n'], item['v'])
        end
      end
    end

    def find(uuid)
      record = @query_executor.find_one(table_name, uuid: uuid)
      ::EventLog::Event.from_record(record)
    end

    def publish(type, data = nil, date = Time.now)
      event = ::EventLog::Event.new(type: type, data: data, date: date)
      event.tap do
        operations = build_index_batch_operations(event)
        event_record = event.as_record
        @connection_pool.with_connection do |conn|
          conn.batch_write_item(
            request_items: {
              table_name => [{ put_request: { item: event_record } }],
              index_table_name => operations
            }
          )
        end
      end
    end

    private

    def count_from_partition(partition, from, to, options = {})
      criteria = {
        table_name: index_table_name,
        consistent_read: false,
        key_condition_expression: '#n = :n AND #v BETWEEN :from AND :to',
        expression_attribute_names: { '#n' => 'n', '#v' => 'v' },
        expression_attribute_values: {
          ':n' => "#{partition.name},#{partition.timestamp}",
          ':from' => \
            if options.key?(:exclusive_start_key)
              options[:exclusive_start_key].to_s + '/'
            else
              "#{from.to_i * 1000}/"
            end,
          ':to' => "#{to.to_i * 1000}/"
        }
      }

      @query_executor.count(criteria)
    end

    def find_events_from_partition(partition, from, to, options = {})
      criteria = {
        table_name: index_table_name,
        consistent_read: false,
        key_condition_expression: '#n = :n AND #v BETWEEN :from AND :to',
        expression_attribute_names: { '#n' => 'n', '#v' => 'v' },
        expression_attribute_values: {
          ':n' => "#{partition.name},#{partition.timestamp}",
          ':from' => \
            if options.key?(:exclusive_start_key)
              options[:exclusive_start_key].to_s + '/'
            else
              "#{from.to_i * 1000}/"
            end,
          ':to' => "#{to.to_i * 1000}/"
        },
        projection_expression: '#v',
        scan_index_forward: \
          options.fetch(:order, :asc).to_sym == :desc ? false : true
      }

      max_items = options.fetch(:limit, -1).to_i

      Enumerator.new do |arr|
        @query_executor.find_all(criteria).each do |items|
          uuids = items.collect { |item| item['v'] }.slice(
            0, max_items.positive? ? max_items : items.length
          )
          @event_fetcher_pool.batch_get_events(uuids).each do |item|
            arr << item
            max_items -= 1
          end
          break if max_items.zero?
        end
      end
    end

    def build_index_batch_operations(event)
      [
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
              v: event.uuid
            }
          }
        },
        {
          put_request: {
            item: {
              n: "#{namespace},partitions,#{event.type},#{event.time_partition}",
              v: event.uuid
            }
          }
        },
        {
          put_request: {
            item: { n: "#{namespace},event_types", v: event.type }
          }
        }
      ]
    end
  end
end
