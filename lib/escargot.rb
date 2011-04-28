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
    return @elastic_search_client unless @elastic_search_client.nil?
    begin
      @elastic_search_client = ElasticSearch.new('localhost:9200')
    rescue ElasticSearch::ConnectionFailed
      nil
    end
  end
  def self.elastic_search_client= new_client
    @elastic_search_client = new_client
  end

  def self.index_name
    @index_name ||= "escargot_#{Rails.env}"
  end
  def self.index_name= new_index_name
    @index_name = new_index_name
  end

  def self.index_options
    @index_options ||= {}
  end
  def self.index_options= new_options
    new_options.symbolize_keys!
    @index_options = new_options
  end

  def self.allowed_index_policies
    [:immediate, :immediate_with_refresh, :enqueue]
  end

  Error = Class.new(StandardError)
end
