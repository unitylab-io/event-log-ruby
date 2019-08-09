module EventLog
  class TimePartition
    attr_reader :namespace, :date, :type

    TIME_LENGTH = 1_209_600

    def self.timestamp_for(date)
      ts = date.to_i
      ts - (ts % TIME_LENGTH)
    end

    def self.from_record(record)
      new(record['ns'], Time.at(record['date']), record['type'])
    end

    def initialize(namespace, date, type)
      @namespace = namespace.to_s
      @date = date
      @type = type.to_s
    end

    def id
      "#{@namespace},#{timestamp},#{@type}"
    end

    def timestamp
      @timestamp ||= @date.to_i
    end

    def cover?(date)
      timestamp >= date.to_i
    end
  end
end
