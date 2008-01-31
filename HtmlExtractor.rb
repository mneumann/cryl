#
# Extract information out of an HTML page, like the page's title, the
# titles of anchor and image tags, all text elements, links or meta
# tags.
#
# It uses a very fast (and low memory overhead) streaming Html parser.
#
#
# Copyright (c) 2007, 2008 by Michael Neumann (mneumann@ntecs.de)
#
class HtmlExtractor
  require 'hpricot'

  attr_reader :meta, :title, :anchor_links, :image_links, :texts, :titles

  def initialize(html)
    @html = html
    extract()
  end

  private

  def extract
    @title = "" 
    @meta = [] 
    @anchor_links = []
    @image_links = []
    @texts = []
    @titles = []

    in_title = false
    ignore_text = false

    Hpricot.scan(@html) do |type, name, attr, str|
      case type
      when :procins
      when :text
        next if ignore_text
        if in_title
          @title << str
          in_title = false
        else
          @texts << str
        end
      when :emptytag, :stag
        case name
        when /^script$/i
          ignore_text = true if type == :stag
        when /^title$/i
          in_title = 3 if type == :stag
        when /^meta$/i 
          @meta << attr
        when /^a$/i
          attr.each do |k,v|
            if k =~ /^href$/i and v and v !~ /^#/
              @anchor_links << v 
            end
            @titles << v if k =~ /^title$/i
          end
        when /^img$/i
          attr.each do |k,v|
            @image_links << v if k =~ /^src$/i
            @titles << v if k =~ /^title$/i
          end
        end
      when :etag, 
        ignore_text = false
      when :comment, :doctype
      else
      end

      if in_title
        in_title -= 1
        in_title = false if in_title <= 0
      end
    end
  end
end # class HtmlExtractor

if __FILE__ == $0
  require 'open-uri'
  puts HtmlExtractor.new(open('http://www.ntecs.de/').read).texts.join
end
