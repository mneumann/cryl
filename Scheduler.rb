# 
# The class that schedules the insertion of new tasks into the event
# queue.
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class Scheduler
  require 'rev'
  require 'VictimCache'
  require 'WorkerQueue'
  require 'HttpUrl'
  require 'HttpClient'
  require 'set'

  DNS_VICTIM_CACHE_SIZE = 1000

  class Task < Struct.new(:http_url, :ip, :failure); end

  #
  # Note: Watcher detaches itself from the event loop, so we don't have
  # to do that ourself.
  #
  class DnsResolver < Rev::DNSResolver
    def initialize(scheduler, task)
      @scheduler, @task = scheduler, task
      super(@task.http_url.host)
    end

    def on_success(ip_addr)
      @task.ip = ip_addr
      @scheduler.dns_complete(@task)
    end

    def on_failure
      @scheduler.dns_failed(@task)
    end
  end

  class HttpHandler < ::HttpClient::Handler
    def initialize(scheduler, task, basename)
      @scheduler, @task, @basename = scheduler, task, basename
      @io = File.open(@basename + '.err', 'w')
      super(@task.http_url.host, @task.http_url.request_uri)
    end

    def error(reason)
      @task.failure = reason || :fetch_failed
      @io.close rescue nil
      @scheduler.fetch_done(@task)
    end

    def header(hash)
      # TODO:
    end

    def body(data)
      @io.write(data)
    end

    def success
      @io.close
      File.rename(@basename + '.err', @basename)
      @scheduler.fetch_done(@task)
    end
  end

  def initialize(node_manager, max_tasks)
    @node_manager = node_manager
    @event_loop = Rev::Loop.new
    
    #
    # A simple victim cache to cache domainname to IP address mappings.
    #
    @dns_victim_cache = VictimCache.new(DNS_VICTIM_CACHE_SIZE) 
    
    #
    # Contain all domains that couldn't be resolved. Used to skip
    # failing domains. 
    #
    @dns_failures = Set.new

    @dns_worker_queue = WorkerQueue.new(max_tasks)
    @fetch_worker_queue = WorkerQueue.new(max_tasks) 

    #
    # Contains all IP addresses to which we are currently connected
    # (HTTP connection)
    #
    @current_connections = Hash.new
  end

  def run
    while true
      @event_loop.run_once
      step()
      break if @event_loop.watchers.empty?
    end
  end

  def step
    #
    # move DNS resolved tasks into the fetch queue.
    #
    @dns_worker_queue.transfer(@fetch_worker_queue) do |task|
      if @current_connections.include?(task.ip)
        # Skip it (for now) if there is a HTTP connection to this IP
        false
      else
        fetch(task)
        true
      end
    end
   
    while not @dns_worker_queue.full?
      if http_url = get_next_url()
        if ip = to_ip(http_url)
          # We already know the IP of the domain, so we
          # need not resolve it. As such we can enqueue it
          # directly into the output queue.
          @dns_worker_queue.enqueue_into_output(Task.new(http_url, ip))
        elsif @dns_failures.include?(http_url.host)
          @node_manager.log_error :dns_failure, http_url.to_s.inspect
        else
          # We have to start a resolver task.
          task = Task.new(http_url)
          @dns_worker_queue.enqueue(task)
          dns_resolve(task)
        end
      else
        break # there are no more URLs available
      end
    end
  end

  def dns_complete(task)
    @dns_worker_queue.complete(task) # move task to output queue
    @dns_victim_cache.put([task.http_url.host, task.ip])
  end

  def dns_failed(task)
    @node_manager.log_error :dns_failure, task.http_url.to_s.inspect
    @dns_worker_queue.remove_from_input(task)
    @dns_failures.add(task.http_url.host)
  end

  def dns_resolve(task)
    # TODO: test if the hostname is currently resolved. ignore for now!
    attach_job(DnsResolver.new(self, task))
  end

  # 
  # done == success or failure
  # 
  def fetch_done(task)
    @fetch_worker_queue.remove_from_input(task)
    @current_connections.delete(task.ip)
    if task.failure
      @node_manager.log_error task.failure, task.http_url.to_s.inspect
    else
      @node_manager.log :success, task.http_url.to_s.inspect
    end
  end

  def fetch(task)
    @current_connections[task.ip] = task
    basename = @node_manager.ensure_filename(task.http_url.to_filename)
    attach_job(HttpClient.new(task.ip, task.http_url.port,
                              HttpHandler.new(self, task, basename)))
  end

  protected

  def attach_job(job)
    job.attach(@event_loop)
  end

  #
  # Returns the next HttpUrl that is neither invalid nor already
  # fetched. If there are no more URLs, it returns +nil+.
  #
  def get_next_url
    while true
      unless url = @node_manager.dequeue_url
        # there are currently no further URLs in the queue
        return nil
      end

      unless http_url = HttpUrl.parse(url)
        @node_manager.log_error :invalid_url, url.inspect 
        next
      end

      if @node_manager.has_file?(http_url.to_filename) 
        @node_manager.log :skip_url__already_fetched, url.inspect
        next
      end

      return http_url
    end # while
  end

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
