describe('LevelDB', function()
  local db = LevelDB.new('./tmp/test')

  before_each(function()
    local iter = db:newIterator()
    iter:first()
    for k, v in iter.next, iter do db:del(k) end
  end)

  teardown(function()
    db:close()
  end)

  describe(':get/set', function()
    it('should can get/set data', function()
      db:set('a', '1')
      db:set('b', '12')
      t = tostring(os.time())
      db:set('t', t)
      assert.is_equal(db:get('a'), '1')
      assert.is_equal(db:get('b'), '12')
      assert.is_equal(db:get('t'), t)
    end)

    it('should return nil if get a empty key', function()
      assert.is_equal(db:get('unset_a'), nil)
      assert.is_equal(db:get('unset_b'), nil)
    end)
  end)

  describe(':batchSet', function()
    it('should can batch set data', function()
      t = tostring(os.time())
      db:set('ab', '123')
      db:batchSet({ba = '1111', bb = '1234', t = t})
      assert.is_equal(db:get('ab'), '123')
      assert.is_equal(db:get('ba'), '1111')
      assert.is_equal(db:get('bb'), '1234')
      assert.is_equal(db:get('t'), t)
    end)
  end)

  describe(':del', function()
    it('should can delete data by key', function()
      t = tostring(os.time())
      db:batchSet({ba = '1111', bb = '1234', t = t})
      db:del('ba')
      assert.is_equal(db:get('ba'), nil)
      assert.is_equal(db:get('bb'), '1234')
      assert.is_equal(db:get('t'), t)
    end)
  end)

  describe(':batchDel', function()
    it('should can delete data by a list of key', function()
      t = tostring(os.time())
      db:batchSet({ba = '1111', bb = '1234', t = t})
      db:batchDel({'ba', 'bb'})
      assert.is_equal(db:get('ba'), nil)
      assert.is_equal(db:get('bb'), nil)
      assert.is_equal(db:get('t'), t)
    end)
  end)

  describe('Iterator', function()
    it('should allow map all data from first to last', function()
      db:batchSet({a = '1', b = '2', c = '4'})
      local iter = db:newIterator()
      iter:first()
      local kvs = {}
      for k, v in iter.next, iter do table.insert(kvs, {k, v}) end
      assert.is_same(kvs, { {'a', '1'}, {'b', '2'}, {'c', '4'} })
      iter:destroy()
    end)

    it('should allow map all data from first to first', function()
      db:batchSet({a = '1', b = '2', c = '4'})
      local iter = db:newIterator()
      iter:first()
      local kvs = {}
      for k, v in iter.prev, iter do table.insert(kvs, {k, v}) end
      assert.is_same(kvs, { {'a', '1'} })
      iter:destroy()
    end)

    it('should allow map from last to last', function()
      db:batchSet({a = '1', b = '2', c = '4'})
      local iter = db:newIterator()
      iter:last()
      local kvs = {}
      for k, v in iter.next, iter do table.insert(kvs, {k, v}); end
      assert.is_same(kvs, {{'c', '4'} })
      iter:destroy()
    end)

    it('should allow map from last to first', function()
      db:batchSet({a = '1', b = '2', c = '4'})
      local iter = db:newIterator()
      iter:last()
      local kvs = {}
      for k, v in iter.prev, iter do table.insert(kvs, {k, v}); end
      assert.is_same(kvs, { {'c', '4'}, {'b', '2'}, {'a', '1'} })
      iter:destroy()
    end)

    it('should allow map from a key to last', function()
      db:batchSet({b = '2', e = '1'})
      db:batchSet({a = '12', d = '14'})
      local iter = db:newIterator()
      iter:seek('c')
      local kvs = {}
      for k, v in iter.next, iter do table.insert(kvs, {k, v}); end
      assert.is_same(kvs, { {'d', '14'}, {'e', '1'} })
      iter:destroy()
    end)

  end)
end)
