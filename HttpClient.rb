#
# A HttpClient library using Rev and the Mongrel HTTP parser.
#
# Much more lightweight than the one shipped with Rev, for example DNS
# resolving is not built, i.e. only IP addresses are accepted.
#
# TODO:
#
#   * Check HTTP return code (200)
#   * Redirects
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#

require 'RevSocket'
require 'http11_client'
require 'time'

class HttpClient < RevSocket
  CONTENT_LENGTH = 'CONTENT_LENGTH'
  TRANSFER_ENCODING = 'TRANSFER_ENCODING'

  class HttpHash < Hash
    attr_accessor :http_reason, :http_status, :http_version
    attr_accessor :http_body, :http_chunk_size, :last_chunk

    def chunk_size
      @chunk_size ||= @http_chunk_size ? @http_chunk_size.to_i(16) : 0
    end

    def clear_chunk_size
      @chunk_size = @http_chunk_size = nil
    end

    def content_length
      Integer(self[CONTENT_LENGTH]) rescue nil
    end

    def chunked_encoding?
      self[TRANSFER_ENCODING] =~ /chunked/i
    end

    def inspect
      h = {}; h.update(self)
      instance_variables.each{|k| h[k] = instance_variable_get(k) }
      h.inspect
    end
  end

  def initialize(ip_addr, port)
    @parser = Rev::HttpClientParser.new
    @parser_pos = 0
    @header = HttpHash.new
    @state = :read_header
    super(ip_addr, port)
  end

  def send_request(host, request_uri)
    str = 
    "GET #{request_uri} HTTP/1.1\r\n" \
    "Host: #{host}\r\n"               \
    "Date: #{Time.now.httpdate}\r\n"   \
    "Content-Length: 0\r\n"            \
    "User-Agent: Ruby/Cryl\r\n"        \
    "Connection: close\r\n\r\n"
    write(str)
  end

  def on_close
    @parser.finish
    super
  end

  def on_read_header
    return false unless http_parse()

    # TODO:
    # check request
    #
    @store_into = open_store()
    
    @state = 
      if @header.chunked_encoding?
        :read_chunk_header
      elsif @bytes_remaining = @header.content_length
        if @bytes_remaining <= 0
          :complete
        else
          :read_body
        end
      else
        :read_body_until_close
      end
      
    return true
  end

  def on_read_body_until_close
    @read_buffer.write_to(@store_into) if @store_into
    false
  end

  def on_read_body
    read_bytes_remaining(:complete)
  end

  def on_read_chunk_trailer
    # TODO
    on_complete()
  end

  def on_read_chunk_header
    @header.clear_chunk_size
    return false unless http_parse()

    @bytes_remaining = @header.chunk_size
    @state =
      if @bytes_remaining == 0
        :read_chunk_trailer
      else
        :read_chunk
      end

    true
  end

  def on_read_chunk
    read_bytes_remaining(:read_chunk_header)
  end

  def on_read
    return false if @read_buffer.empty?
    #while send((c="on_#{@state}"; p c; c)); end
    while send("on_#{@state}"); end
  end

  def on_complete
    on_close()
    @store_into.close if @store_into
    false
  end

  def open_store
    STDOUT
    #File.open('/tmp/output.abc', 'w+')
  end

  def read_bytes_remaining(complete_state)
    str = @read_buffer.read(@bytes_remaining)
    @store_into.write(str) if @store_into
    @bytes_remaining -= str.size
    if @bytes_remaining <= 0
      @state = complete_state
      true
    else
      false
    end
  end

  def http_parse
    begin
      # FIXME .to_str is slow!
      @parser_pos = @parser.execute(@header, @read_buffer.to_str, @parser_pos)
    rescue Rev::HttpClientParserError
      on_error(:http_parse_error)
      return false
    end

    if @parser.finished?
      @read_buffer.read(@parser_pos)
      @parser_pos = 0
      @parser.reset
      true
    else
      false
    end
  end

end

if __FILE__ == $0
  evloop = Rev::Loop.new
  c = HttpClient.new('www.ntecs.de', 80)
  c.send_request('www.ntecs.de', '/')
  c.attach(evloop)
  evloop.run
end
