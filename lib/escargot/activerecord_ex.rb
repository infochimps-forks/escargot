require 'will_paginate/collection'

module Escargot
  module ActiveRecordExtensions

    def self.included(base)
      base.send :extend, ClassMethods
    end

    module ClassMethods
      
      def index_name
        @index_name ||= Escargot.index_name
      end
      attr_writer :index_name

      def index_options
        @index_options ||= Escargot.index_options
      end
      attr_writer :index_options
      
      def elastic_search_type
        @elastic_search_type ||= self.name.underscore.singularize.gsub(/\//,'-')
      end
      attr_writer :elastic_search_type

      def update_index_policy
        @update_index_policy ||= :immediate
      end
      def update_index_policy= new_index_policy
        raise Escargot::Error.new("'#{new_index_policy}' is not a valid index policy; must be one of #{allowed_index_policies.join(', ')}") unless Escargot.allowed_index_policies.include?(new_index_policy.to_s.to_sym)
      end
      
      attr_accessor :mapping
      
      # defines an elastic search index. Valid options:
      #
      # :index_name (will default class name using method "underscore")
      #
      # :updates, how to to update the contents of the index when a document is changed, valid options are:
      #
      #   - false: do not update the index
      #
      #   - :immediate: update the index but do not refresh it automatically.
      #     With the default settings, this means that the change may take up to 1 second
      #     to be seen by other users.
      #
      #     See: http://www.elasticsearch.com/docs/elasticsearch/index_modules/engine/robin/
      #
      #     This is the default option.
      #
      #   - :immediate_with_refresh: update the index AND ask elasticsearch to refresh it after each
      #     change. This garantuees that the changes will be seen by other users, but may affect
      #     performance.
      #
      #   - :enqueu: enqueue the document id so that a remote worker will update the index
      #     This is the recommended options if you have set up a job queue (such as Resque)
      #

      def elastic_index(options = {})
        Escargot.register_model(self)

        options.symbolize_keys!
        
        self.index_name          = options[:index_name]    if options[:index_name].present?
        self.elastic_search_type = options[:type]          if options[:type].present?
        self.update_index_policy = options[:updates]       if options[:updates].present?
        self.index_options       = options[:index_options] if options[:index_options].present?
        self.mapping             = options[:mapping]       if options[:mapping].present?                                                                
        
        send :include, InstanceMethods
        
        if update_index_policy
          after_save :update_index
          after_destroy :delete_from_index
        end

      end

      # search_hits returns a raw ElasticSearch::Api::Hits object for the search results
      # see #search for the valid options
      def search_hits(query, options = {})
        Escargot.elastic_search_client.search(query, options.merge({:index => self.index_name, :type => elastic_search_type}))
      end

      # search returns a will_paginate collection of ActiveRecord objects for the search results
      #
      # see ElasticSearch::Api::Index#search for the full list of valid options
      #
      # note that the collection may include nils if ElasticSearch returns a result hit for a
      # record that has been deleted on the database
      def search(query, options = {})
        hits = search_hits(query, options)
        hits_ar = hits.map{ |hit| hit.to_activerecord(options[:find_options]) }
        results = WillPaginate::Collection.new(hits.current_page, hits.per_page, hits.total_entries)
        results.replace(hits_ar)
        results
      end

      # counts the number of results for this query.
      def search_count(query = "*", options = {})
        Escargot.elastic_search_client.count(query, options.merge({:index => self.index_name, :type => elastic_search_type}))
      end

      def facets(fields_list, options = {})
        size = options.delete(:size) || 10
        fields_list = [fields_list] unless fields_list.kind_of?(Array)
        
        if !options[:query]
          options[:query] = {:match_all => true}
        elsif options[:query].kind_of?(String)
          options[:query] = {:query_string => {:query => options[:query]}}
        end

        options[:facets] = {}
        fields_list.each do |field|
          options[:facets][field] = {:terms => {:field => field, :size => size}}
        end

        hits = Escargot.elastic_search_client.search(options, {:index => self.index_name, :type => elastic_search_type})
        out = {}
        
        fields_list.each do |field|
          out[field.to_sym] = {}
          hits.facets[field.to_s]["terms"].each do |term|
            out[field.to_sym][term["term"]] = term["count"]
          end
        end

        out
      end

      # explicitly refresh the index, making all operations performed since the last refresh
      # available for search
      #
      # http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/refresh/
      def refresh_index(index_version = nil)
        Escargot.elastic_search_client.refresh(index_version || index_name)
      end
      
      # creates a new index version for this model and sets the mapping options for the type
      def create_index_version
        index_version = Escargot.elastic_search_client.create_index_version(index_name, index_options)
        if mapping
          Escargot.elastic_search_client.update_mapping(mapping, :index => index_version, :type => elastic_search_type)
        end
        index_version
      end
      
      # deletes all index versions for this model
      def delete_index
        # deletes any index version
        Escargot.elastic_search_client.index_versions(index_name).each do |index_version|
          Escargot.elastic_search_client.delete_index(index_version)
        end
        
        # and delete the index itself if it exists
        begin
          Escargot.elastic_search_client.delete_index(index_name)
        rescue ElasticSearch::RequestError
          # it's ok, this means that the index doesn't exist
        end
      end
      
      def delete_id_from_index(id, options = {})
        options[:index] ||= self.index_name
        options[:type]  ||= elastic_search_type
        if Escargot.elastic_search_client
          Escargot.elastic_search_client.delete(id.to_s, options)
        else
          Rails.logger.debug("Could not delete #{options[:type]} #{id}; no ElasticSearch client")
        end
      end
      
      def optimize_index
        Escargot.elastic_search_client.optimize(index_name)
      end

      module InstanceMethods

        # override this method to skip indexing on an instance by instance basis
        def skip_indexing?
          false
        end
        
        # updates the index using the appropiate policy
        def update_index
          return if skip_indexing?
          if self.class.update_index_policy == :immediate_with_refresh
            local_index_in_elastic_search(:refresh => true)
          elsif self.class.update_index_policy == :enqueue
            Resque.enqueue(DistributedIndexing::ReIndexDocuments, self.class.to_s, [self.id])
          else
            local_index_in_elastic_search
          end
        end

        # deletes the document from the index using the appropiate policy ("simple" or "distributed")
        def delete_from_index
          if self.class.update_index_policy == :immediate_with_refresh
            self.class.delete_id_from_index(self.id, :refresh => true)
            # As of Oct 25 2010, :refresh => true is not working
            self.class.refresh_index()
          elsif self.class.update_index_policy == :enqueue
            Resque.enqueue(DistributedIndexing::ReIndexDocuments, self.class.to_s, [self.id])
          else
            begin
              self.class.delete_id_from_index(self.id)
            rescue ElasticSearch::RequestError => e
              nil
            end
          end
        end

        def json_doc_to_index
          return indexed_json_document if respond_to?(:indexed_json_document) # backwards compatibility
          respond_to?(:doc_to_index) ? self.doc_to_index.to_json : self.to_json
        end

        # options to use when indexing a record.  override this method
        # to return a Hash with keys like :_parent, :op_type, &
        # :_routing to access ElasticSearch's more advanced options.
        def indexing_options
          {}
        end

        def local_index_in_elastic_search(options = {})
          options[:index] ||= self.class.index_name
          options[:type]  ||= self.class.elastic_search_type
          options[:id]    ||= self.id.to_s

          if Escargot.elastic_search_client
            Escargot.elastic_search_client.index(self.json_doc_to_index, options.merge(indexing_options))
          else
            Rails.logger.debug("Could not index #{self.class} #{id}; no ElasticSearch client")
          end
          
          ## !!!!! passing :refresh => true should make ES auto-refresh only the affected
          ## shards but as of Oct 25 2010 with ES 0.12 && rubberband 0.0.2 that's not the case
          if options[:refresh]
            self.class.refresh_index(options[:index])
          end
          
        end

        def more_like_this options={}
          raise Escargot::Error.new("Must specify an Array :fields to match against") unless options[:fields] && (!options[:fields].empty?) && options[:fields].is_a?(Array)
          options[:like_text] ||= options[:fields].map { |f| send(f).to_s }.join(' ')
          allowed_mlt_options = %w[fields like_text percent_terms_to_match min_term_freq max_query_terms stop_words min_doc_freq max_doc_freq min_word_len max_word_len boost_terms boost].map(&:to_sym)
          mlt_options = {}.tap do |o|
            options.each_pair { |k, v| o[k] = v if allowed_mlt_options.include?(k) }
          end
          self.class.search({:query => {:more_like_this => mlt_options}}, options)
        end

        def fuzzy_like_this options={}
          raise Escargot::Error.new("Must specify an Array :fields to match against") unless options[:fields] && (!options[:fields].empty?) && options[:fields].is_a?(Array)
          options[:like_text] ||= options[:fields].map { |f| send(f).to_s }.join(' ')
          allowed_flt_options = %w[fields like_text ignore_tf max_query_terms boost].map(&:to_sym)
          flt_options = {}.tap do |o|
            options.each_pair { |k, v| o[k] = v if allowed_flt_options.include?(k) }
          end
          self.class.search({:query => {:fuzzy_like_this => flt_options}}, options)
        end
        
        
      end
    end
  end
end
