#
# Fetches a URL.
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class Fetcher
  require 'net/http'
  require 'timeout'

  #
  # opts::
  #
  #   :timeout
  #   :headers
  #   :min_size
  #   :max_size
  #   :valid_content_types = [regexp, regexp]
  #
  def self.fetch(domain_name, port, path, query, opts={})
    url = ""
    url << path
    if query
      url << "?" 
      url << query
    end

    ctype, body = nil, nil

    begin
      Timeout.timeout(opts[:timeout]) do
        Net::HTTP.start(domain_name, port) do |http|
          http.request(Net::HTTP::Get.new(url, opts[:headers])) do |response|

            status = response.code.to_i
            # TODO: handle redirects
            return :invalid_status, status if status < 200 or status >= 300 

            length = response.content_length
            return :page_too_small, length if opts[:min_size] and length < opts[:min_size]
            return :page_too_large, length if opts[:max_size] and length > opts[:max_size]

            ctype = response.content_type.downcase
            if opts[:valid_content_types]
              return :invalid_content_type, ctype unless opts[:valid_content_types].any? {|pat| pat =~ ctype}
            end

            body = response.read_body
          end
        end
      end
    rescue Timeout::Error
      return :timeout
    end

    return :ok, ctype, body

  rescue Exception => e
    return :unknown_error, e
  end
end

if __FILE__ == $0
  p Fetcher.fetch('www.ntecs.de', 80, '/', nil, :timeout => 0.06) {|ctype, body|
    p ctype
    p body
  }
end
