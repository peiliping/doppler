local resty_redis = require "resty.redis"
local redis       = resty_redis:new()
redis:set_timeout(10000)
local ok , err    = redis:connect("127.0.0.1", 6379)
if err then ngx.say(err) return end

local ngx_re      = require "ngx.re"
local json        = require "cjson"
local scripts     = ngx.shared.scripts

local file = io.open("/home/peiliping/dev/logs/" .. ngx.var.arg_filename , "r")
local lineNum , page = 1 , 10

for line in file:lines() do
  if lineNum % page == 1 then
    redis:init_pipeline()
  end
  if lineNum > 1 then
    local item = ngx_re.split(line , "\t")
    --redis:evalsha(sha , 3 , "test" , 20170912 , item[1] , item[2] , item[3] , item[4] , item[5] , item[6] , item[7] , item[8] , item[9] , item[10] , item[11])
    redis:evalsha(scripts:get("WriteScript") , 4 , "test" , 0 , item[1] , 0 , item[4])
  end
  if lineNum % page == 0 then
    redis:commit_pipeline()
  end
  lineNum = lineNum + 1
end
ngx.say(lineNum)
redis:close()