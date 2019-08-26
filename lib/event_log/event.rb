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
      @date = attributes.fetch(:date, Time.now)
      @type = attributes.fetch(:type)
      @data = attributes.fetch(:data, nil)
      @checksum = Base64.urlsafe_encode64(
        Digest::SHA1.digest(JSON.dump(@data))
      ).slice(0, 27)
      @uuid = attributes.fetch(:uuid) do
        format(
          '%d/%s',
          (@date.to_f * 1000).to_i,
          Base64.urlsafe_encode64(Digest::SHA1.digest("#{@type},#{@checksum}"))
        )
      end
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
        'ds' => JSON.dump(@data)
      }
    end
  end
end
