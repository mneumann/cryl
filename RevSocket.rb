#
# RevSocket 
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#

require 'rev'
require 'socket'

class RevSocket < Rev::IOWatcher
  include Socket::Constants

  BLOCK_SIZE = 16 * 1024

  attr_reader :socket
  attr_reader :read_buffer, :write_buffer

  def initialize(ip_addr, port)
    @ip_addr, @port = ip_addr, port
    @socket = Socket.new(AF_INET, SOCK_STREAM, 0)
    @sockaddr = Socket.sockaddr_in(@port, @ip_addr)
    @connected = false
    @read_buffer = Rev::Buffer.new 
    @write_buffer = Rev::Buffer.new

    on_writeable() # connect
    super(@socket, 'rw') 
  end

  def connected?
    @connected
  end

  def on_readable
    if @connected
      if @socket.eof?
        on_close()
      else
        on_can_read()
      end
    end
  rescue => e
    STDERR.puts "unexpected_error: #{e}"
    on_error(:unexpected_error)
  end

  def on_writeable
    if @connected
      on_can_write()
    else
      begin
        @socket.connect_nonblock(@sockaddr)
        @connected = true
      rescue Errno::EINPROGRESS
      rescue Errno::EISCONN
        @connected = true
      rescue 
        on_error(:connection_failure)
      end
      on_connect() if @connected
    end
  rescue => e
    STDERR.puts "unexpected_error: #{e}"
    on_error(:unexpected_error)
  end

  alias on_writable on_writeable

  #
  #
  #
  
  def write(data)
    @write_buffer.append(data)
  end

  alias << write

  protected

  def on_can_write()
    unless @write_buffer.empty?
      begin
        @write_buffer.write_to(@socket) 
      rescue
        on_error(:write_failure)
      end
    end
  end

  def on_can_read()
    #
    # FIXME: use @read_buffer.read_from(@socket).
    #
    begin
      data = @socket.read_nonblock(BLOCK_SIZE)
    rescue
      on_error(:read_failure)
      return
    end

    if data and data.size > 0
      @read_buffer.append(data)
      on_read()
    end
  end

  def on_connect()
  end

  def on_read()
  end

  def on_close()
    cleanup()
  end

  def on_error(reason)
    cleanup()
  end

  def cleanup
    @socket.close unless @connected
    @connected = false
    @read_buffer.clear
    @write_buffer.clear
    detach()
  end
end

if __FILE__ == $0
  class S < RevSocket
    def on_connect
      self.write "GET / HTTP/1.0\r\n\r\n"
    end
    def on_read()
      puts @read_buffer.to_str
    end
  end

  evloop = Rev::Loop.new
  S.new('127.0.0.1', 9999).attach(evloop)
  evloop.run
end
