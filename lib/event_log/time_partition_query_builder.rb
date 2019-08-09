module EventLog
  class TimePartitionQueryBuilder
    def initialize(namespace)
      @namespace = namespace
    end

    def as_query(event, from, to = Time.now)
      pkey_start = ::EventLog::TimePartition.timestamp_for(from.to_i)
      pkey_end = to.to_i
      {
        key_condition_expression: '#n = :n AND #v BETWEEN :from AND :to',
        expression_attribute_names: { '#n' => 'n', '#v' => 'v' },
        expression_attribute_values: {
          ':n' => "#{@namespace},partitions,#{event}",
          ':from' => pkey_start.to_s,
          ':to' => pkey_end.to_s
        }
      }
    end
  end
end
