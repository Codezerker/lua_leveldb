LevelDB = require 'leveldb'
db = LevelDB.new('./tmp')

print('version: ' .. db.version .. "\n")

print('Get k1 => ' .. (db:get('k1') or "'nil'"))
print('Get k2 => ' .. (db:get('k2') or "'nil'"))
print('Get unset_key => ' .. (db:get('unset_key') or "'nil'"))
print("")

function print_db_data()
  print('Iterator all keys')
  db:each(nil, function(k, v) print(k, v) end)
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

print('destory')
LevelDB.destroy_db('./tmp')

print('end')

