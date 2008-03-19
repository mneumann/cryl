#
# Code for parsing/normalizing URLs 
#
# TODO: resolve . and ..
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class HttpUrl

  attr_accessor :host, :port, :path, :query, :original_url
  attr_writer :ip

  require 'uri'
  require 'cgi'
  require 'digest/sha1'

  def self.parse(href, base_url=nil)
    return nil if href.nil?

    url = self.new
    url.original_url = href 

    href = href.dup
    href.strip!

    href.tr!(" \t\r\n", "++++") # escape whitespace

    begin
      uri = URI.parse(href)
    rescue URI::Error
      return nil
    end

    # 
    # We only handle http URLs, no https or other.
    #
    return nil if uri.scheme and uri.scheme !~ /^http$/i

    #
    # Host
    #
    url.host = uri.host
    if url.host.nil?
      # 
      # If no host is specified, we have to use the host
      # of <tt>base_url</tt> if given.
      #
      return nil if base_url.nil?
      url.host = base_url.host
    end
    return nil if url.host.nil?
    url.host = url.host.downcase

    #
    # Port
    #
    url.port = uri.port
    if url.port.nil?
      return nil if base_url.nil?
      url.port = base_url.port
    end

    #
    # Path
    #
    path = uri.path 
    return nil if path.nil? # it's an invalid URL!
    if path.empty?

      #
      # http://www.ntecs.de # => scheme="http", path=""
      # ?query              # => scheme=nil, path=""
      #
      if uri.scheme
        path = "/"
      else
        # relative URL
        return nil if base_url.nil?
        path = base_url.path 
      end
    elsif path !~ /^\//
      # relative path
      return nil if base_url.nil?
      return nil if base_url.path.nil?
      return nil if base_url.path !~ /^\// 
      path = base_url.path[0..(base_url.path.rindex("/"))] + path
    else
      # absolute path
    end

    url.path = path

    #
    # Query (normalize)
    #
    if query = uri.query
      begin
        params = CGI.parse(query) 
        new_query = []
        params.keys.sort.each do |k|
          params[k].sort.each do |v|
            if v
              new_query << "#{CGI.escape(k)}=#{CGI.escape(v)}" 
            else
              new_query << CGI.escape(k)
            end
          end
        end
        query = new_query.join("&")
      rescue Exception 
      end
    end
    url.query = query

    #
    # Assert
    #
    raise if url.host.nil? or url.port.nil? or url.path.nil? or url.path.empty? or url.path !~ /^\//

    return url
  rescue => ex
    nil
  end

  def pathquery 
    str = "#{@path}"
    if @query
      str << "?"
      str << @query
    end
    str
  end

  alias request_uri pathquery

  def host_ip_addr?
    @host =~ /^\d+[.]\d+[.]\d+[.]\d+$/ ? true : false
  end

  def ip
    return @ip if @ip
    return @ip = @host if host_ip_addr?
    @ip = HttpUrl.dns_resolve(@host)
  end

  def to_filename
    a = @host.split(".").reverse.map {|s| s.gsub!(/[^A-Za-z0-9_-]*/, '') }
    a << Digest::SHA1.hexdigest(self.to_s)
    a.join('/')
  end

  def to_a
    [@host, @port, @path, @query]
  end

  def to_s
    "http://#{@host}:#{@port}#{pathquery()}"
  end

  def ==(o)
    return false unless o.is_a?(HttpUrl)
    @host == o.host and @port == o.port and @path == o.path and @query == o.query
  end
end

def aggregate_links(out=STDOUT)
  while links = gets
    links.chomp!
    next unless File.exist?(links)
    ext = File.extname(links) 
    base = links[0..(-1-ext.size)]
    base_url = File.read(base + ".url") rescue nil

    IO.foreach(links) do |line|
      line.chomp!
      if url = HttpUrl.parse(line, base_url)
        out.puts url.to_s
      end
    end
  end
end

aggregate_links() if __FILE__ == $0
