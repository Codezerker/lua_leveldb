describe('LevelDB', function()
  local db = LevelDB.new('./tmp/test')

  before_each(function()
    db:each(nil, function(k, v) db:del(k) end)
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

    it('should can delete a unset key', function()
      db:del('del_unset_key')
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
      local kvs = {}
      db:newIteratorWith(nil, function(iter)
        iter:first()
        for k, v in iter.next, iter do table.insert(kvs, {k, v}) end
      end)
      assert.is_same(kvs, { {'a', '1'}, {'b', '2'}, {'c', '4'} })
    end)

    it('should allow map all data from first to first', function()
      db:batchSet({a = '1', b = '2', c = '4'})
      local kvs = {}
      db:newIteratorWith(nil, function(iter)
        iter:first()
        for k, v in iter.prev, iter do table.insert(kvs, {k, v}) end
      end)
      assert.is_same(kvs, { {'a', '1'} })
    end)

    it('should allow map from last to last', function()
      db:batchSet({a = '1', b = '2', c = '4'})
      local kvs = {}
      db:newIteratorWith(nil, function(iter)
        iter:last()
        for k, v in iter.next, iter do table.insert(kvs, {k, v}); end
      end)
      assert.is_same(kvs, {{'c', '4'} })
    end)

    it('should allow map from last to first', function()
      db:batchSet({a = '1', b = '2', c = '4'})
      local kvs = {}
      db:newIteratorWith(nil, function(iter)
        iter:last()
        for k, v in iter.prev, iter do table.insert(kvs, {k, v}); end
      end)
      assert.is_same(kvs, { {'c', '4'}, {'b', '2'}, {'a', '1'} })
    end)

    it('should allow map from a key to last', function()
      db:batchSet({b = '2', e = '1'})
      db:batchSet({a = '12', d = '14'})
      local kvs = {}
      db:newIteratorWith(nil, function(iter)
        iter:seek('c')
        for k, v in iter.next, iter do table.insert(kvs, {k, v}); end
      end)
      assert.is_same(kvs, { {'d', '14'}, {'e', '1'} })
    end)
  end)

  describe(':each', function()
    it('should each all data', function()
      db:batchSet({b = '2', e = '1'})
      db:batchSet({a = '12', d = '14'})
      local kvs = {}
      db:each(nil, function(k, v, iter)
        table.insert(kvs, {k, v})
      end)
      assert.is_same(kvs, { {'a', '12'}, {'b', '2'}, {'d', '14'}, {'e', '1'} })
    end)

    it('should stop each if return false', function()
      db:batchSet({b = '2', e = '1'})
      db:batchSet({a = '12', d = '14'})
      local kvs = {}
      db:each(nil, function(k, v, iter)
        table.insert(kvs, {k, v})
        return false
      end)
      assert.is_same(kvs, { {'a', '12'} })
    end)
  end)
end)
