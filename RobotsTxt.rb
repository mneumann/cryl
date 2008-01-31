#
# Parses a robots.txt file.
#
# Copyright (c) 2007, 2008 by Michael Neumann (mneumann@ntecs.de)
#
class RobotsTxt
  class Rule < Struct.new(:allow, :path); end

  attr_reader :rules, :user_agent_order

  def initialize(str)
    @rules = {}
    @user_agent_order = []
    current_user_agent = nil
    
    str.reject {|line| line =~ /^\s*#/ or line =~ /^\s*$/ }.
      map{|line| line.strip.split(":", 2)}.
      reject {|a| a.size != 2}.
      map{|a| [a[0].strip.downcase, a[1].strip]}.
      each do |key, value|
        case key 
        when 'user-agent'
          current_user_agent = value
          @user_agent_order << current_user_agent
          @rules[current_user_agent] ||= []
        when 'allow'
          @rules[current_user_agent] ||= []
          @rules[current_user_agent] << Rule.new(true, value)
        when 'disallow'
          @rules[current_user_agent] ||= []
          @rules[current_user_agent] << Rule.new(false, value)
        else
          # ignore
        end
      end
  end

  def allowed?(user_agent, path, default=true)
    @user_agent_order.each do |ua|
      if ua == '*' or ua == user_agent
        access = access_rule(@rules[ua]||[], path)
        return access unless access.nil?
      end
    end
    return default
  end

  def access_rule(rules_array, path)
    rules_array.each do |rule|
      if rule.path[-1,1] == '/'
        path.index(rule.path) == 0 and (return rule.allow) 
      else
        path == rule.path and (return rule.allow)
      end
    end
    return nil
  end
end

if __FILE__ == $0
  require 'pp'
  require 'open-uri'
  robots = RobotsTxt.new(open('http://www.heise.de/robots.txt'))
  p robots.allowed?("MS Search 4.0 Robot", "/")
  p robots.allowed?("Googlebot", "/tr/result.xhtml2")
end
