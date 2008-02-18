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

  def initialize(ip_addr, port, filename)
    @parser = Rev::HttpClientParser.new
    @parser_pos = 0
    @header = HttpHash.new
    @filename = filename
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
    super
    if @state == :read_body_until_close
      on_success()
    else
      on_error(:premature_eof)
    end
  end

  def handle_read_header
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
          :success
        else
          :read_body
        end
      else
        :read_body_until_close
      end
      
    return true
  end

  def handle_read_body_until_close
    @read_buffer.write_to(@store_into) if @store_into
    false
  end

  def handle_read_body
    read_bytes_remaining(:success)
  end

  def handle_read_chunk_trailer
    # TODO
    on_success()
    false
  end

  def handle_read_chunk_header
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

  def handle_read_chunk
    read_bytes_remaining(:read_chunk_header)
  end

  def handle_success
    on_success()
    false
  end

  def on_read
    return false if @read_buffer.empty?
    while send("handle_#{@state}"); end
  end

  def on_success
    cleanup()
  end

  def cleanup
    @parser.finish
    @store_into.close if @store_into
    @store_into = nil
    super
  end

  def open_store
    File.open(@filename, "w+")
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
  c = HttpClient.new('www.ntecs.de', 80, "/tmp/downloadit")
  c.send_request('www.ntecs.de', '/')
  c.attach(evloop)
  evloop.run
end
