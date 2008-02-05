#
# A HttpClient library using Rev and the Mongrel HTTP parser.
#
# Much more lightweight than the one shipped with Rev, for example DNS
# resolving is not built, i.e. only IP addresses are accepted.
#
# TODO:
#
#   * Implement Chunk Encoding
#   * Check HTTP return code (200)
#   * Redirects
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#

require 'rev'
require 'socket'
require 'http11_client'

class HttpClient < Rev::IOWatcher
  include Socket::Constants

  attr_reader :state

  HEADER_BLOCK_SIZE = 1024
  BODY_BLOCK_SIZE = 16 * HEADER_BLOCK_SIZE
  CONTENT_LENGTH = 'CONTENT_LENGTH'

  #
  # Handler class. Write your own!  
  #
  class Handler

    def initialize(host, request_uri)
      @host, @request_uri = host, request_uri
    end

    #
    # Is called in case of an error.
    #
    def error(reason)
    end

    #
    # Produces the HTTP request (returns a String)
    #
    def produce_request
      "GET #{@request_uri} HTTP/1.1\r\n" \
      "Host: #{@host}\r\n"               \
      "Content-Length: 0\r\n"            \
      "User-Agent: Ruby/Cryl\r\n"        \
      "Connection: close\r\n\r\n"
    end

    #
    # Called once when the header was completely parsed.
    #
    def header(hash)
    end

    #
    # Called each time a part of the body is received.  
    #
    def body(data)
    end

    #
    # Called once at the end when everything was fine. 
    #
    def success
    end

  end

  def initialize(ip_addr, port, handler)
    @ip_addr, @port = ip_addr, port
    @handler = handler

    @socket = Socket.new(AF_INET, SOCK_STREAM, 0)
    @sockaddr = Socket.sockaddr_in(@port, @ip_addr)

    @http_parser = Rev::HttpClientParser.new
    @header = Hash.new
    @buffer = ""
    @pos = 0

    @state = :connecting
    begin
      @socket.connect_nonblock(@sockaddr)
      @state = :connected
    rescue Errno::EINPROGRESS
    end

    super(@socket, 'rw') 
  end

  def on_readable
    case @state
    when :wait_for_header
      if @socket.eof?
        error(:not_enough_data)
      elsif data = @socket.read_nonblock(HEADER_BLOCK_SIZE)
        @buffer << data

        begin
          @pos = @http_parser.execute(@header, @buffer, @pos)
        rescue Rev::HttpClientParserError
          error(:header_parse_error)
          return
        end

        if @http_parser.finished?
          @http_parser.finish
          @handler.header(@header)

          # Remove Header from buffer
          @buffer = @buffer[@pos..-1] || ""

          @remaining = nil
          if len = @header[CONTENT_LENGTH]
            @remaining = Integer(len)
          end
          
          if @remaining
            @handler.body(@buffer[0, @remaining])  
            @remaining -= @buffer.size
          else
            @handler.body(@buffer)
          end

          # We don't need the buffer any more
          @buffer = nil

          if complete?
            success()
          else
            @state = :wait_for_data
          end
        end
      end
    when :wait_for_data
      if @socket.eof?
        if complete?
          success()
        else
          error(:not_enough_data)
        end
      elsif data = @socket.read_nonblock([BODY_BLOCK_SIZE, @remaining].compact.min)
        @handler.body(data)
        success() if complete?
      end
    end
  rescue => e
    STDERR.puts "unexpected_error: #{e}"
    error(:unexpected_error)
  end

  def on_writable
    case @state
    when :connecting
      begin
        @socket.connect_nonblock(@sockaddr)
      rescue Errno::EISCONN
        @state = :connected
      rescue 
        error(:connection_failed)
      end
    when :connected 
      request = @handler.produce_request
      if @socket.write(request) != request.size
        error(:write_not_completed) 
      else
        @state = :wait_for_header
      end
    end
  rescue => e
    STDERR.puts "unexpected_error: #{e}"
    error(:unexpected_error)
  end

  private

  def complete?
    (@remaining and @remaining <= 0) or (@remaining.nil? and @socket.eof?)
  end

  def success
    @state = :completed
    @socket.close
    detach()
    @handler.success()
  end

  def error(reason=nil)
    @socket.close unless @state == :connecting
    @state = :error
    detach()
    @handler.error(reason)
  end
end

if __FILE__ == $0
  evloop = Rev::Loop.new
  HttpClient.new('127.0.0.1', 8081, HttpClient::Handler.new('localhost', '/test')).attach(evloop)
  evloop.run
end