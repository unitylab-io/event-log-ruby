module EventLog
  class Event
    attr_reader :uuid, :date, :type, :data, :checksum

    def self.from_record(record)
      new(
        uuid: record['uuid'], date: Time.at(record['ts'] / 1000.0),
        type: record['t'], data: JSON.parse(record['ds'])
      )
    end

    def initialize(attributes = {})
      @uuid = attributes.fetch(:uuid, SecureRandom.uuid)
      @date = attributes.fetch(:date, Time.now)
      @type = attributes.fetch(:type)
      @data = attributes.fetch(:data, nil)
      @data_str = JSON.dump(@data)
      @checksum = Base64.urlsafe_encode64(
        Digest::SHA1.digest(@data_str)
      ).slice(0, 27)
    end

    def time_partition
      ::EventLog::TimePartition.timestamp_for(date)
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

    def binary_id
      Digest::SHA256.digest("#{namespace},#{type},#{timestamp},#{@data_str}")
    end

    def id
      Base64.urlsafe_encode64(binary_id).slice(0, 43)
    end

    def timeid
      "#{timestamp}.#{@data_signature}"
    end

    def timestamp
      (@date.to_f * 1000.0).to_i
    end

    def record_row_key
      time_partition_ts = ::EventLog::TimePartition.timestamp_for(date)
      "#{namespace},#{type},#{time_partition_ts}"
    end

    def as_record
      {
        'uuid' => uuid,
        't' => type,
        'ts' => timestamp,
        'ds' => @data_str
      }
    end
  end
end
