module EventLog
  class TimePartition
    attr_reader :name, :date

    TIME_LENGTH = 86_400 # 1 day

    def self.timestamp_for(date)
      ts = date.to_i
      ts - (ts % TIME_LENGTH)
    end

    def self.from_record(record)
      new(record['n'], Time.at(record['v']))
    end

    def initialize(name, date)
      @name = name.to_s
      @date = date
    end

    def timestamp
      @date.to_i
    end
  end
end
