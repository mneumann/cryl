class HashQueue
  def initialize(max_size=nil)
    @max_size = max_size
    @hash = Hash.new
    @size = 0
  end

  def enq(key, value)
    raise "queue full" if full?
    (@hash[key] ||= []) << value
    @size += 1
  end

  def deq(key)
    return nil if empty?
    if q = @hash[key]
      next_element = q.shift
      @size -= 1
      @hash.delete(key) if q.empty?
      return next_element
    else
      nil
    end
  end

  def deq_select(&cond)
    return nil if empty?
    @hash.each_key {|key|
      return deq(key) if cond.call(key)
    }
    return nil
  end

  def empty?
    @size == 0
  end

  def full?
    @max_size and @size >= @max_size 
  end
end
