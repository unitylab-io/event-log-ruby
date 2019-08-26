module EventLog
  class QueryExecutor
    def initialize(options)
      @options = options
    end

    def find_one(table_name, item_key)
      build_dynamodb_client.get_item(
        table_name: table_name,
        key: item_key
      ).item
    end

    def count(criteria)
      dynamodb_client = build_dynamodb_client
      criteria = criteria.merge(select: 'COUNT')
      total = 0
      loop do
        resp = dynamodb_client.query(criteria)
        total += resp.count
        break if resp.last_evaluated_key.nil?

        criteria[:exclusive_start_key] = resp.last_evaluated_key
      end
      total
    end

    def find_all(criteria)
      dynamodb_client = build_dynamodb_client

      Enumerator.new do |arr|
        loop do
          resp = dynamodb_client.query(criteria)
          resp.items.each { |item| arr << item }
          break if resp.last_evaluated_key.nil?

          criteria[:exclusive_start_key] = resp.last_evaluated_key
        end
      end
    end

    private

    def build_dynamodb_client
      Aws::DynamoDB::Client.new(@options.fetch(:dynamodb_config, {}))
    end
  end
end
