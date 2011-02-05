# Escargot
require 'elasticsearch'
require 'escargot/activerecord_ex'
require 'escargot/elasticsearch_ex'
require 'escargot/local_indexing'
require 'escargot/distributed_indexing'
require 'escargot/queue_backend/base'
require 'escargot/queue_backend/resque'

module Escargot
  def self.register_model(model)
    @indexed_models ||= []
    @indexed_models << model if !@indexed_models.include?(model)
  end

  def self.indexed_models
    @indexed_models || []
  end

  def self.queue_backend
    @queue ||= Escargot::QueueBackend::Rescue.new
  end

  def self.elastic_search_client
    @elastic_search_client ||= ElasticSearch.new('localhost:9200')
  end

  def self.elastic_search_client= new_client
    @elastic_search_client = new_client
  end
end
