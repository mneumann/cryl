#
# DNS resolver.
#
# Automatically detaches from the event loop.
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#

require 'rev'
require 'socket'
require 'resolv'

class DnsResolver < Rev::IOWatcher
  TIMEOUT = 4
  MAX_REQS = 10
  MAX_RETRIES = 4
  MAX_NS = 5

  class Timeout < Rev::TimerWatcher
    def initialize(interval, repeating, &timeout) 
      @timeout = timeout
      super(interval, repeating)
    end
    def on_timer; @timeout.call() end
  end

  def self.ns_from_resolv_conf(file='/etc/resolv.conf')
    File.read(file).scan(/^\s*nameserver\s+(.+)\s*$/).flatten
  end

  def initialize(hostname, nameservers=DnsResolver.ns_from_resolv_conf(),
                 max_reqs=MAX_REQS, max_retries=MAX_RETRIES,
                 use_syscall=true, &notify) 
    @hostname, @nameservers = hostname, nameservers.cycle
    @max_reqs = max_reqs
    @max_retries = max_retries
    @use_syscall = use_syscall
    @notify = notify

    @socket = UDPSocket.new
    @orig_hostname = hostname
    @reqs = 0
    @tries = 0
    @timer = Timeout.new(TIMEOUT, repeating=true) { on_timeout() }
    super(@socket)
  end

  def attach(evloop)
    @timer.attach(evloop)
    send_request()
    super
  end

  def detach
    @timer.detach if @timer.attached?
    super
  end

  def on_readable
    begin
      message = decode_message(@socket.recvfrom_nonblock(Resolv::DNS::UDPSize).first)
    rescue
      on_failure(:recv_failed)
      return
    end

    if message and message.id == @request.id
      additional_map = {} 
      message.each_additional do |name, ttl, data|
        case data
        when Resolv::DNS::Resource::IN::A
          additional_map[name.to_s] = data.address.to_s
        end
      end

      cname = nil
      message.each_answer do |name, ttl, data|
        case data
        when Resolv::DNS::Resource::IN::A
          on_success(data.address.to_s)
          return
        when Resolv::DNS::Resource::IN::CNAME
          cname = data.to_s
          if address = additional_map[cname]
            on_success(address)
            return
          end
        end
      end

      nameservers = []
      message.each_authority do |name, ttl, data|
        case data
        when Resolv::DNS::Resource::IN::NS
          ns = data.name.to_s
          nameservers << (additional_map[ns] || ns)
        end
      end

      unless nameservers.empty? 
        send_request(cname, nameservers.sort)
        return
      end
    end

    send_request!
  end

  def on_timeout
    STDERR.puts "WARN: DnsResolver: TIMEOUT for #{@current_ns}"
    on_failure(:timeout)
  end

  def on_success(address)
    detach()
    @notify.call(true, address) 
  end

  def on_failure(reason)
    @tries += 1
    if @tries < @max_retries
      # retry
      send_request()
      return
    end
    detach()

    #
    # If everything fails, try to get the hostname via a blocking
    # syscall.
    #
    if @use_syscall
      STDERR.puts "WARN: DnsResolver: fall back to gethostbyname - #{@orig_hostname}"
      ip = Socket.gethostbyname(@orig_hostname).last.unpack('C*').join('.') rescue nil
      if ip
        @notify.call(true, ip) 
      else
        @notify.call(false, :gethostbyname_failed) 
      end
    else
      @notify.call(false, reason) 
    end
  end

  protected

  def send_request(hostname=nil, nameservers=nil)
    @timer.reset
    @hostname = hostname || @hostname
    @nameservers = nameservers.cycle if nameservers

    if @reqs >= @max_reqs
      on_failure(:max_reqs_reached)
      return
    end

    found = false
    n = 0
    @nameservers.each do |ns|
      begin
        @socket.connect(ns, Resolv::DNS::Port)
        @current_ns = ns
        break
      rescue
        STDERR.puts "WARN: DnsResolver: connect failed to #{ ns }"
      end
      if (n += 1) >= MAX_NS
        on_failure(:connection_failed)
        return
      end
    end

    send_request!
  end

  def send_request!
    @request = Resolv::DNS::Message.new
    @request.add_question(@hostname, Resolv::DNS::Resource::IN::ANY)
    @socket.send(@request.encode, 0)
    @reqs += 1
  end

  def decode_message(datagram)
    Resolv::DNS::Message.decode(datagram)
  rescue
    nil
  end

end

if __FILE__ == $0
  evloop = Rev::Loop.new
  DnsResolver.new('www.ntecs.de') {|ok, address| p address if ok }.attach(evloop)
  evloop.run
end
