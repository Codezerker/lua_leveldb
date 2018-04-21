Lua LevelDB
===========

This is a FFI library, provide some simple interface.

## Requirements

* LuaJIT 


## APIs

__DB Class__

* open(name, options)
* destroy_db(name)


__DB Instrace__ 

* get(key)
* set(key, val)
* del(key)
* batchSet({k1 = 'v', k2 = 'v2'})
* batchDel({'k1', 'k2'})
* newIterator(options)
* close()


__Iterator__

* first()
* last()
* seek(key)
* next()
* prev()
* destroy()


## Example

Example

```
leveldb = require 'leveldb'
db = leveldb.new('./tmp')

print('version: ' .. db.version .. "\n")

print('Get k1 => ' .. db:get('k1'))
print('Get k2 => ' .. db:get('k2'))
print('Get unset_key => ' .. (db:get('unset_key') or "'nil'"))
print("")

function print_db_data()
  print('Iterator all keys')
  local iter = db:new_iterator()

  iter:first()
  for k, v in iter.next, iter do
    print(k, v)
  end
  iter:destroy()
  print("")
end

print("Set k3")
db:set('k3', tostring(os.time()))
print("")

print("Batch set k1 k2 k4 k5")
db:batchSet({k1 = tostring(os.time()), k2 = '321321', k4 = '111', k5 = '222'})
print("")

print_db_data()

print("Del k3")
db:del('k3')
print("")

print_db_data()

print("Batch del k4, k5")
db:batchDel({'k4', 'k5'})
print("")

print_db_data()

print('close')
db:close()
```


## Test

```
bin/busted
```
