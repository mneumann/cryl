#
# Extract links out of an HTML page and builds absolute URLs.
#
# It uses a very fast (and low memory overhead) streaming Html parser.
#
# Copyright (c) 2007, 2008 by Michael Neumann (mneumann@ntecs.de)
#
class LinkExtractor
  require 'hpricot'
  require 'uri'
  require 'cgi'
  require 'set'

  def self.extract(html, doc_host, doc_port, doc_path, &block)
    doc_host = doc_host.downcase
    doc_dir = doc_path[0..doc_path.rindex('/')]

    if block.nil?
      block = proc { true }
    end
    link_set = Set.new

    Hpricot.scan(html) do |type, name, attrs, str|
      case type
      when :emptytag, :stag
        case name
        when 'a', 'A'
          if href = attrs['href'] || attrs['HREF'] || attrs['Href']
            href.strip!
            next if href.empty?
            next if href =~ /^#/
            next if href == '/'   # we already visited the root.

            #
            # construct absolute URL
            #

            href.tr!(" \t\r\n", "++++") # escape whitespace
            
            begin
              uri = URI.parse(href)
            rescue URI::Error
              next
            end

            next if uri.scheme and uri.scheme !~ /^http$/i

            host = uri.host
            if host
              host.downcase!
            else
              host = doc_host
            end

            port = uri.port || doc_port 
            path = uri.path
            query = uri.query

            if path.nil? or path.empty? or path !~ /^\//
              # relative path
              path = doc_dir + (path || "")
            else
              # absolute path
              path = path || "/" 
            end

            #
            # normalize query (sort)
            #
            if query
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

            link = [host, port, path, query]
            link_set.add(link) if block.call(link)
          end
=begin
        when /^base$/i
          if href = attrs['href'] || attrs['HREF'] || attrs['Href']
            href.strip!
            base = href 
          end
=end
        end
      end
    end
    return link_set
  end
end

if __FILE__ == $0
  body = File.read('/tmp/test.html')
  p LinkExtractor.extract(body, 'ntecs.de', 80, "/abc") 
  #{|host, port, path, query|
  #  p host, port, path, query
  #  true
  #}
end
