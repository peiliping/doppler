local resty_redis = require "resty.redis"
local redis       = resty_redis:new()
redis:set_timeout(10000)
local ok , err    = redis:connect("127.0.0.1", 6379)
if err then ngx.say(err) return end

local r = redis:script("FLUSH")
ngx.say(r)
redis:set_keepalive(10000, 100)