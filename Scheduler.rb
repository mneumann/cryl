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
  require 'set'

  DNS_VICTIM_CACHE_SIZE = 1000

  class Task < Struct.new(:http_url, :ip); end

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
  end

  def run
    while true
      step()
      @event_loop.run_once
      break if @event_loop.watchers.empty?
    end
  end

  def step
    #
    # move DNS resolved tasks into the fetch queue.
    #
    @dns_worker_queue.transfer(@fetch_worker_queue) do |task|
      # TODO:
      # test for connections. if there is already a connection to
      # the same IP then skip it 
      puts "OK: #{task.http_url.host} -> #{task.ip}"
      true
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

  def attach_job(job)
    job.attach(@event_loop)
  end

  protected

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
