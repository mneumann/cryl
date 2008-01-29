#
# Extract anchors links out of an HTML page.
#
# It uses a very fast (and low memory overhead) streaming Html parser.
#
# Copyright (c) 2007, 2008 by Michael Neumann (mneumann@ntecs.de)
#
class LinkExtractor
  require 'hpricot'

  include Enumerable

  def initialize(html)
    @html = html
  end

  def each
    Hpricot.scan(@html) do |type, name, attrs, str|
      case type
      when :emptytag, :stag
        case name
        when 'a', 'A'
          if href = attrs['href'] || attrs['HREF'] || attrs['Href'] || attrs['HRef']
            #
            # omit empty links
            #
            next if href.nil?
            next if href =~ /^\s*(#(.*))?$/ 

            yield href
          end
        end
      end
    end
  end
end
