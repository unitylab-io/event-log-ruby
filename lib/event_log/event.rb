module EventLog
  class Event
    attr_reader :type, :data
    attr_accessor :namespace, :date

    def self.from_record(partition, record)
      timestamp = record['tid'].split('.').first.to_i
      new(
        partition.namespace, partition.type, Time.at(timestamp / 1000.0),
        JSON.parse(record['ds'])
      )
    end

    def initialize(namespace, type, date, data = nil)
      @date = date
      self.namespace = namespace
      self.type = type
      self.data = data
    end

    def time_partition
      ::EventLog::TimePartition.new(
        namespace, ::EventLog::TimePartition.timestamp_for(date), type
      )
    end

    def namespace=(arg)
      unless arg.is_a?(String) && arg.index(',').nil?
        raise ArgumentError, 'invalid namespace'
      end

      @namespace = arg
    end

    def type=(arg)
      unless arg.is_a?(String) && arg.index(',').nil?
        raise ArgumentError, 'invalid event type'
      end

      @type = arg
    end

    def id
      Base64.urlsafe_encode64(
        Digest::SHA256.digest("#{namespace},#{type},#{timestamp},#{@data_str}")
      ).slice(0, 43)
    end

    def timeid
      "#{timestamp}.#{@data_signature}"
    end

    def timestamp
      (@date.to_f * 1000.0).to_i
    end

    def data=(arg)
      unless arg.nil? || arg.is_a?(Hash)
        raise ArgumentError, '`Hash` or `nil` expected'
      end

      @data_str = JSON.dump(arg)
      @data_signature = Base64.urlsafe_encode64(
        Digest::SHA1.digest(@data_str)
      ).slice(0, 27)
      @data = arg
    end

    def as_record
      time_partition_ts = ::EventLog::TimePartition.timestamp_for(date)
      {
        'rk' => "#{namespace},#{type},#{time_partition_ts}",
        'tid' => "#{timestamp}.#{@data_signature}",
        'ds' => JSON.dump(data)
      }
    end
  end
end
