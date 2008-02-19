class FetchManager
  require 'rev'
  require 'HttpUrl'
  require 'HttpClient'
  require 'HashQueue'

  class MyHttpClient < HttpClient
    attr_accessor :fetch_manager, :http_url

    def on_error(reason)
      super
      @fetch_manager.on_error(reason, self)
    end

    def on_success()
      super
      @fetch_manager.on_success(self)
    end
  end

  def initialize(node_manager, max_connections=1000, max_queue_size=10_000)
    @node_manager = node_manager
    @max_connections = max_connections

    # The key is in all cases the IP.
    @connections = Hash.new
    @queue = HashQueue.new(max_queue_size)

    @event_loop = Rev::Loop.new

    @state = :normal 
  end

  def run
    loop do
      if @state == :normal
        while new_job(); end
      end
      break if @state == :queue_empty and @event_loop.watchers.empty?
      p @event_loop.watchers.size
      @event_loop.run
    end
  end

  def new_job
    return false if @queue.full?

    http_url = @node_manager.get_next_url

    if http_url.nil?
      @state = :queue_empty
      return false
    end

    if ip = http_url.ip
      if connections_busy?(ip)
        # queue request
        @queue.enq(ip, http_url)
      else
        # start request
        start_job(http_url)
      end
    else
      @node_manager.log_error(:dns_lookup_failed, http_url.inspect)
    end

    return true
  end

  def choose_next_job(ip)
    @queue.deq(ip) || @queue.deq_select {|k| !@connections.include?(k) }
  end

  def start_job(http_url)
    return if http_url.nil?
    #puts "start_job" #: #{http_url}"

    add_connection(http_url)

    c = MyHttpClient.new(http_url.ip, http_url.port, @node_manager.ensure_filename(http_url.to_filename))
    c.fetch_manager = self
    c.http_url = http_url
    c.send_request(http_url.host, http_url.request_uri)
    c.attach(@event_loop)
    p c
  end

  def on_error(reason, client)
    http_url = client.http_url
    @node_manager.log_error :fetch_failed, http_url.inspect

    delete_connection(http_url)
    start_job(choose_next_job(http_url.ip))
  end

  def on_success(client)
    http_url = client.http_url
    @node_manager.log :success, http_url.inspect

    delete_connection(http_url)
    start_job(choose_next_job(http_url.ip))
  end

  def add_connection(http_url)
    raise if http_url.ip.nil?
    raise if @connections.include?(http_url.ip)
    @connections[http_url.ip] = http_url
  end

  def delete_connection(http_url)
    @connections.delete(http_url.ip)
  end

  def connections_busy?(ip)
    @connections.size >= @max_connections or @connections.include?(ip)
  end

end
