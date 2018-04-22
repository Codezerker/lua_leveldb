leveldb = require 'leveldb'
db = leveldb.new('./tmp')

print('version: ' .. db.version .. "\n")

counter = 0

while true do
  db:get('k1')
  db:get('k2')
  db:get('unset_key')

  db:set('k1', tostring(os.time()))
  db:set('k2', tostring(os.time()))
  db:set('k3', tostring(os.time()))

  db:batchSet({k1 = tostring(os.time()), k2 = '321321', k4 = '111', k5 = '222'})
  db:batchSet({k1 = tostring(os.time()), k2 = '321321', k4 = '111', k5 = '222'})
  db:batchSet({k1 = tostring(os.time()), k2 = '321321', k4 = '111', k5 = '222'})

  db:del('k1')
  db:del('k2')
  db:del('k3')

  db:batchDel({'k4', 'k5', 'k6'})
  db:batchDel({'k4', 'k5', 'k6'})
  db:batchDel({'k4', 'k5', 'k6'})

  counter = counter + 15
  io.write("\r - " .. counter .. ' - ')
  if counter % 100000 == 0 then
    print("\n" .. collectgarbage('count') .. "\n")
  end
end

print('close')
db:close()

