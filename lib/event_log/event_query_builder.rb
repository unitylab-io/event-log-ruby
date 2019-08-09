module EventLog
  class EventQueryBuilder
    attr_accessor :from, :to

    def initialize(time_partition)
      @time_partition = time_partition
    end

    def as_query(from, to = Time.now)
      @expression_attribute_names = { '#rk' => 'rk' }
      @expression_attribute_values = { ':rk' => @time_partition.id }
      {
        key_condition_expression: key_condition_expression_builder(from, to),
        expression_attribute_names: @expression_attribute_names,
        expression_attribute_values: @expression_attribute_values
      }
    end

    private

    def key_condition_expression_builder(from, to)
      tid_start = nil
      tid_end = nil
      ['#rk = :rk'].tap do |arr|
        tid_start = from.to_i * 1000 if @time_partition.cover?(from)
        tid_end = to.to_i * 1000 if @time_partition.cover?(to)

        next if tid_start.nil? && tid_end.nil?

        @expression_attribute_names['#tid'] = 'tid'

        if !tid_start.nil? && !tid_end.nil?
          @expression_attribute_values[':tid_start'] = "#{tid_start}."
          @expression_attribute_values[':tid_end'] = "#{tid_end}."
          arr << '#tid BETWEEN :tid_start AND :tid_end'
        elsif !tid_start.nil?
          @expression_attribute_values[':tid_start'] = "#{tid_start}."
          arr << '#tid >= :tid_start'
        elsif !tid_start.nil?
          @expression_attribute_values[':tid_end'] = "#{tid_end}."
          arr << '#tid <= :tid_end'
        end
      end.join(' AND ')
    end
  end
end
