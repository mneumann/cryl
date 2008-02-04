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
  TIMEOUT = 3
  MAX_REQS = 5 

  class Timeout < Rev::TimerWatcher
    def initialize(ref, interval, repeating) 
      @ref = ref
      super(interval, repeating)
    end
    def on_timer
      @ref.on_timeout
    end
  end

  def self.ns_from_resolv_conf(file='/etc/resolv.conf')
    File.read(file).scan(/^\s*nameserver\s+(.+)\s*$/).flatten.last
  end

  def initialize(hostname, nameserver=DnsResolver.ns_from_resolv_conf(),
                 max_reqs=MAX_REQS, use_syscall=true, &notify) 
    @hostname, @nameserver = hostname, nameserver
    @orig_hostname = hostname
    @socket = UDPSocket.new
    @max_reqs, @reqs = max_reqs, 0
    @use_syscall = use_syscall
    @notify = notify
    @timer = Timeout.new(self, TIMEOUT, repeating=true)
    super(@socket)
  end

  def attach(evloop)
    @timer.attach(evloop)
    send_request()
    super
  end

  def detach
    @timer.detach if @timer and @timer.attached?
    super
  end

  def on_readable
    message = decode_message(@socket.recvfrom_nonblock(Resolv::DNS::UDPSize).first)

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

      if cname
        send_request(cname)
        return
      end

      ns = nil
      message.each_authority do |name, ttl, data|
        case data
        when Resolv::DNS::Resource::IN::NS
          ns = data.name.to_s
          if ns_addr = additional_map[ns] 
            send_request(nil, ns_addr)
            return
          end
        end
      end

      if ns
        send_request(nil, ns)
        return
      end
    end

    on_failure(:invalid_message)
  end

  def on_timeout
    on_failure(:timeout)
  end

  def on_success(address)
    detach()
    @notify.call(true, address) 
  end

  def on_failure(reason)
    if @reqs < @max_reqs
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

  def send_request(hostname=nil, nameserver=nil)
    @timer.reset
    @hostname = hostname || @hostname
    @nameserver = nameserver || @nameserver

    if @reqs >= @max_reqs
      on_failure(:max_reqs_reached)
      return
    end

    @request = Resolv::DNS::Message.new
    @request.add_question(@hostname, Resolv::DNS::Resource::IN::A)
    @socket.connect(@nameserver, Resolv::DNS::Port)
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
