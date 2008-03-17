#
# A SizedQueue is like an array of a fixed size into which you can only
# insert elements if it's not full.
#
# Note, +nil+ shouldn't be used as element.
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class SizedQueue
  def initialize(max_size)
    @max_size = size
    @array = Array.new
  end

  def full?
    @array.size >= @max_size
  end

  #
  # Tries to fill the queue will the elements produces by
  # calling the block. Breaks the loop on +nil+.
  #
  def fill
    while not full? 
      if item = yield
        enqueue(item)
      else
        break
      end
    end
  end

  def enqueue(item)
    raise if full?
    @array.push(item)
  end

  #
  # Return +nil+ when empty.
  #
  def dequeue
    @array.shift
  end
end
