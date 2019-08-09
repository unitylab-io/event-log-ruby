require 'base64'
require 'json'
require 'digest/sha1'
require 'aws-sdk-dynamodb'
require 'event_log/version'
require 'event_log/event'
require 'event_log/time_partition'
require 'event_log/event_query_builder'
require 'event_log/time_partition_query_builder'
require 'event_log/client'

module EventLog
  class Error < StandardError; end
end
