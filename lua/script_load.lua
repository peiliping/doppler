local resty_redis = require "resty.redis"
local redis       = resty_redis:new()
redis:set_timeout(10000)
local ok , err    = redis:connect("127.0.0.1", 6379)
if err then ngx.say(err) return end

local scripts = ngx.shared.scripts

local scriptWrite = redis:script("LOAD" , 
[[
  local blockSize , blockPower , intSize = 2^15 , 15 , 2^31
  local expireTime = 7 * 86400

  local tableName , dateTime , uidNumber , append = KEYS[1] , tonumber(KEYS[2]) , tonumber(KEYS[3]) , KEYS[4] == "1"
  local expire = (dateTime > 0)
  local blockSeq , lineSeq 

  if uidNumber >= intSize then
  	blockSeq , lineSeq = math.modf(uidNumber / blockSize) , uidNumber % blockSize
  else
  	blockSeq , lineSeq = bit.rshift(uidNumber , blockPower) , bit.band(uidNumber , blockSize - 1)
  end
  
  local key4N  = table.concat({tableName , "-" , dateTime , "-Nest"})
  local keyT4V = {tableName , "-" , dateTime , "-" , 5 , "-" , "VS"}
  local keyT4M = {tableName , "-" , dateTime , "-" , 5 , "-" , 7 , "-BMP-" , blockSeq}
  
  local write = function write(keyT4V , keyT4M , index , val , lineSeq , expireTime)
    local oldBit
    keyT4V[5] , keyT4M[5] = index , index
    local key4V = table.concat(keyT4V)
    local isExist = redis.call("SISMEMBER" , key4V , val)
    if isExist == 1 then
      keyT4M[7] = val
      local key4M = table.concat(keyT4M)
      oldBit = redis.call("setbit" , key4M , lineSeq , 1)  
    else
      local count = redis.call("SCARD" , key4V)
      if count >= 1024 then
        val = "IGNORE"
      end
      if count <= 1024 then 
        redis.call("SADD" , key4V , val)
      end      
      if count == 0 and expire then
        redis.call("expire" , key4V , expireTime)
      end
      keyT4M[7] = val
      local key4M = table.concat(keyT4M)
      oldBit = redis.call("setbit" , key4M , lineSeq , 1)
      if expire then
        redis.call("expire" , key4M , expireTime)
      end
    end
    return oldBit
  end

  local oldBitNum = 0
  if append then  
    for seq = 1 , #ARGV , 2 do
      oldBitNum = oldBitNum + write(keyT4V , keyT4M , ARGV[seq] , ARGV[seq + 1] , lineSeq , expireTime)
    end
  else
    for index , val in ipairs(ARGV) do
      oldBitNum = oldBitNum + write(keyT4V , keyT4M , index , val , lineSeq , expireTime) 
    end
  end
  if oldBitNum == 0 then
    redis.call("setbit" , key4N , blockSeq , 1)
  end
  return true
]]
)

scripts:set("WriteScript", scriptWrite)
ngx.say("WriteScript : " .. scriptWrite)

local scriptRead = redis:script("LOAD" , 
[[
  local result = 0
  local tableName , dateTime = KEYS[1] , KEYS[2]
  
  local keyT4N = {tableName , "-" , dateTime , "-Nest"}
  local key4N = table.concat(keyT4N)

  local process = cjson.decode(ARGV[1])
  local recyple = cjson.decode(ARGV[2])
  for index , value in ipairs(process) do
    if value[1] == "BITOP" then
      local params = {value[1] , value[2] , value[3]}
      for seq = 4 , #value , 2 do
        if value[seq] == "b" then
          table.insert(params , {tableName , "-" , dateTime , "-" , value[seq + 1] , "-BMP-" , 7})
        elseif value[seq] == "t" then
          table.insert(params , value[seq + 1])
        end
      end
      process[index] = params
    elseif value[1] == "BITCOUNT" then
      local params = {value[1]}
      if value[2] == "b" then
          table.insert(params , {tableName , "-" , dateTime , "-" , value[3] , "-BMP-" , 7})
        elseif value[2] == "t" then
          table.insert(params , value[3])
        end
      process[index] = params
    end
  end
  
  local bytepos = 0
  local pos = redis.call("bitpos" , key4N , 1 , bytepos)
  while (pos >= 0)
  do
    for g = pos , pos + 7 - bit.band(pos , 7) do
      local fill = redis.call("getbit" , key4N , g)
      if fill == 1 then
        local t = 0
        for index , value in ipairs(process) do
          local ps = {}
          for id , item in ipairs(value) do
            if type(item) == "table" then
              item[7] = g
              ps[id] = table.concat(item)
            else
              ps[id] = item
            end
          end
          t = redis.call(unpack(ps))
        end
        result = result + t
      end
    end
    bytepos = bit.rshift(pos , 3) + 1 
    pos = redis.call("bitpos" , key4N , 1 , bytepos)
  end
  if #recyple > 0 then
    redis.call("del" , unpack(recyple))
  end
  return result
]]
)

scripts:set("ReadScript" , scriptRead)
ngx.say("ReadScript : " .. scriptRead)

redis:set_keepalive(10000, 100)