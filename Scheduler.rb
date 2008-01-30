# 
# The class that schedules the insertion of new tasks into the event
# queue.
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class Scheduler
  require 'VictimCache'
  require 'HttpUrl'
  require 'set'

  DNS_VICTIM_CACHE_SIZE = 1000

  def initialize(node_manager)
    @node_manager = node_manager 

    #
    # A simple victim cache to cache domainname to IP address mappings.
    #
    @dns_victim_cache = VictimCache.new(DNS_VICTIM_CACHE_SIZE) 

    #
    # Contain all domain names that are currently being resolved.
    # This set is used to avoid starting to resolve the same domain 
    # name more than once at the same time.
    #
    @dns_active_set = Set.new 

    #
    # Contain all domains that couldn't be resolved.
    #
    @dns_failures = Set.new

    #
    # Contain all IP addresses to which we are currently connected.
    # Used to avoid more than one connection to a server at the same time.
    #
    @current_connections = Set.new
  end

  def get_next_task
    while true
      url = @node_manager.dequeue_url
      return nil unless url # currently, there are no further URLs in the queue

      http_url = HttpUrl.parse(url)
      unless http_url
        @node_manager.log_error :invalid_url, url.inspect 
        next
      end

      if @node_manager.has_file?(http_url.to_filename) 
        @node_manager.log :skip_url__already_fetched, url.inspect
        next
      end

      # 
      # Determine if there is a task of the same domain running 
      #
      if ip = to_ip(http_url)
        # We have an IP address, so we can skip DNS resolving.
        if @current_connections.include?(ip)
          # There is a connection to the same server right now.
          # To avoid concurrent connections, we put that URL back
          # into the queue.
          @node_manager.enqueue_url(url)
          @node_manager.log :skip_url__concurrent_connection, url.inspect
        else
          # No connection to that IP currently exist.
          # TODO: return a Task object.
          return http_url
        end
      elsif @dns_failures.include?(http_url.host)
        @node_manager.log_error :dns_failure, url.inspect
      elsif @dns_active_set.include?(http_url.host)
        @node_manager.log :skip_url__dns_active_set, url.inspect
      else
        return http_url
      end

    end # while 
  end

  private

  def to_ip(http_url)
    if http_url.host_ip_addr?
      http_url.host
    elsif item = @dns_victim_cache.get(http_url.host) 
      item[1]
    else
      nil
    end
  end

end
