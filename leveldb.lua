local ffi = require "ffi"
local leveldb = ffi.load('leveldb', true)
if not leveldb then error("load leveldb failed.") end
local M = {}
local Iterator = {}

-- Ref https://github.com/google/leveldb/blob/master/include/leveldb/c.h
ffi.cdef[[
  typedef struct leveldb_t leveldb_t;
  typedef struct leveldb_options_t leveldb_options_t;
  typedef struct leveldb_iterator_t leveldb_iterator_t;
  typedef struct leveldb_readoptions_t leveldb_readoptions_t;
  typedef struct leveldb_writebatch_t leveldb_writebatch_t;
  typedef struct leveldb_writeoptions_t leveldb_writeoptions_t;

  leveldb_t* leveldb_open(const leveldb_options_t* options, const char* name, char** errptr);
  void leveldb_destroy_db(const leveldb_options_t* options, const char* name, char** errptr);
  void leveldb_close(leveldb_t* db);
  void leveldb_free(void* ptr);
  int leveldb_major_version();
  int leveldb_minor_version();

  void leveldb_put(
    leveldb_t* db, const leveldb_writeoptions_t* options,
    const char* key, size_t keylen, const char* val, size_t vallen,
    char** errptr
  );
  void leveldb_delete(
    leveldb_t* db, const leveldb_writeoptions_t* options,
    const char* key, size_t keylen,
    char** errptr
  );
  void leveldb_write(
    leveldb_t* db, const leveldb_writeoptions_t* options,
    leveldb_writebatch_t* batch, char** errptr
  );
  char* leveldb_get(
    leveldb_t* db, const leveldb_readoptions_t* options,
    const char* key, size_t keylen, size_t* vallen, char** errptr
  );

  leveldb_iterator_t* leveldb_create_iterator(leveldb_t* db, const leveldb_readoptions_t* options);
  void leveldb_iter_destroy(leveldb_iterator_t*);

  unsigned char leveldb_iter_valid(const leveldb_iterator_t*);
  void leveldb_iter_seek_to_first(leveldb_iterator_t*);
  void leveldb_iter_seek_to_last(leveldb_iterator_t*);
  void leveldb_iter_seek(leveldb_iterator_t*, const char* k, size_t klen);
  void leveldb_iter_next(leveldb_iterator_t*);
  void leveldb_iter_prev(leveldb_iterator_t*);
  const char* leveldb_iter_key(const leveldb_iterator_t*, size_t* klen);
  const char* leveldb_iter_value(const leveldb_iterator_t*, size_t* vlen);
  void leveldb_iter_get_error(const leveldb_iterator_t*, char** errptr);

  leveldb_writebatch_t* leveldb_writebatch_create();
  void leveldb_writebatch_destroy(leveldb_writebatch_t*);
  void leveldb_writebatch_clear(leveldb_writebatch_t*);
  void leveldb_writebatch_put(
    leveldb_writebatch_t*,
    const char* key, size_t klen, const char* val, size_t vlen
  );
  void leveldb_writebatch_delete(leveldb_writebatch_t*, const char* key, size_t klen);

  leveldb_options_t* leveldb_options_create();
  void leveldb_options_destroy(leveldb_options_t*);
  void leveldb_options_set_create_if_missing(leveldb_options_t*, unsigned char);
  void leveldb_options_set_error_if_exists(leveldb_options_t*, unsigned char);
  void leveldb_options_set_compression(leveldb_options_t*, int);

  leveldb_readoptions_t* leveldb_readoptions_create();
  void leveldb_readoptions_destroy(leveldb_readoptions_t*);

  leveldb_writeoptions_t* leveldb_writeoptions_create();
  void leveldb_writeoptions_destroy(leveldb_writeoptions_t*);
]]


local function create_options(options)
  local c_options = leveldb.leveldb_options_create()

  options = options or {}
  if options.create_if_missing then leveldb.leveldb_options_set_create_if_missing(c_options, 1) end
  if options.error_if_exists then leveldb.leveldb_options_set_error_if_exists(c_options, 1) end
  if options.compression then leveldb.leveldb_options_set_compression(c_options, 1) end

  return c_options
end

local function create_read_options()
  return leveldb.leveldb_readoptions_create()
end

local function create_write_options()
  return leveldb.leveldb_writeoptions_create()
end


function M.new(dirname, options)
  local db = {}
  local mt = {__index = M}

  setmetatable(db, mt)

  db.options = options or {create_if_missing = true, error_if_exists = false}

  local c_options = create_options(db.options)
  local c_err = ffi.new("char*[1]")

  db._db = leveldb.leveldb_open(c_options, dirname, c_err)
  leveldb.leveldb_options_destroy(c_options)

  if c_err[0] ~= nil then error(ffi.string(c_err[0])) end

  local major = leveldb.leveldb_major_version()
  local minor = leveldb.leveldb_minor_version()
  db.version = major .. "." .. minor

  return db
end

function M:set(key, val, options)
  local c_options = create_write_options(options)
  local c_err = ffi.new("char*[1]")
  leveldb.leveldb_put(self._db, c_options, key, #key, val, #val, c_err)
  leveldb.leveldb_writeoptions_destroy(c_options)
  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end
end

function M:batchSet(data, options)
  if type(data) ~= "table" then error("data is not a table.") end

  local options = create_write_options(options)
  local batch = leveldb.leveldb_writebatch_create()
  for key, val in pairs(data) do
    leveldb.leveldb_writebatch_put(batch, key, #key, val, #val)
  end
  local c_err = ffi.new("char*[1]")
  leveldb.leveldb_write(self._db, options, batch, c_err)
  leveldb.leveldb_free(batch)
  leveldb.leveldb_writeoptions_destroy(options)
  if c_err[0] ~= nil then error(ffi.string(c_err[0])) end
end

function M:get(key, options)
  local c_options = create_read_options(options)
  local c_err = ffi.new("char*[1]")
  local c_size = ffi.new("size_t[1]")
  local c_result = leveldb.leveldb_get(self._db, c_options, key, #key, c_size, c_err)
  leveldb.leveldb_readoptions_destroy(c_options)

  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  elseif c_size[0] == 0 then
    return nil
  else
    return ffi.string(c_result, c_size[0])
  end
end

function M:del(key, options)
  local c_options = create_write_options(options)
  local c_err = ffi.new("char*[1]")
  leveldb.leveldb_delete(self._db, c_options, key, #key, c_err)
  leveldb.leveldb_writeoptions_destroy(c_options)
  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end
  return true
end

-- Params:
--  data: it's a array, example {'a', 'b', 'c'}
--
function M:batchDel(data, options)
  if type(data) ~= "table" then error("data is not table.") end
  local options = create_write_options(options)
  local batch = leveldb.leveldb_writebatch_create()
  for _, val in ipairs(data) do
    leveldb.leveldb_writebatch_delete(batch, val, #val)
  end
  local c_err = ffi.new("char*[1]")
  leveldb.leveldb_write(self._db, options, batch, c_err)
  leveldb.leveldb_free(batch)
  leveldb.leveldb_writeoptions_destroy(options)
  if c_err[0] ~= nil then error(ffi.string(c_err[0])) end
end

function M:new_iterator(options)
  return Iterator.new(self._db, options)
end

function M:close()
  leveldb.leveldb_close(self._db)
end

function M.destroy_db(dirname)
  local c_options = create_options()
  local c_err = ffi.new("char*[1]")

  leveldb.leveldb_destroy_db(c_options, dirname, c_err)
  leveldb.leveldb_options_destroy(c_options)

  if c_err[0] ~= nil then error(ffi.string(c_err[0])) end
  return true
end


function Iterator.new(db, options)
  local iter = {}
  local mt = {__index = Iterator}

  setmetatable(iter, mt)

  iter._db = db
  local c_options = create_read_options()
  iter.iterator = leveldb.leveldb_create_iterator(iter._db, c_options)
  leveldb.leveldb_readoptions_destroy(c_options)

  return iter
end

function Iterator:first()
  leveldb.leveldb_iter_seek_to_first(self.iterator)
end

function Iterator:last()
  leveldb.leveldb_iter_seek_to_last(self.iterator)
end

function Iterator:seek(key)
  leveldb.leveldb_iter_seek(self.iterator, key, #key)
end

function Iterator:next()
  local valid = leveldb.leveldb_iter_valid(self.iterator)
  if valid == 0 then return nil end

  local c_key_size = ffi.new("size_t[1]")
  local c_key = leveldb.leveldb_iter_key(self.iterator, c_key_size)
  local key = ffi.string(c_key, c_key_size[0])

  local c_value_size = ffi.new("size_t[1]")
  local c_value = leveldb.leveldb_iter_value(self.iterator, c_value_size)
  local value = ffi.string(c_value, c_value_size[0])

  leveldb.leveldb_iter_next(self.iterator)

  return key, value
end

function Iterator:prev()
  local valid = leveldb.leveldb_iter_valid(self.iterator)
  if valid == 0 then return nil end

  local c_key_size = ffi.new("size_t[1]")
  local c_key = leveldb.leveldb_iter_key(self.iterator, c_key_size)
  local key = ffi.string(c_key, c_key_size[0])

  local c_value_size = ffi.new("size_t[1]")
  local c_value = leveldb.leveldb_iter_value(self.iterator, c_value_size)
  local value = ffi.string(c_value, c_value_size[0])

  leveldb.leveldb_iter_prev(self.iterator)

  return key, value
end

function Iterator:destroy()
  if self.iterator then leveldb.leveldb_iter_destroy(self.iterator) end
end


return M
