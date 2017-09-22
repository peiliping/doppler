local ngx_re      = require "ngx.re"
local resty_redis = require "resty.redis"
local redis       = resty_redis:new()
redis:set_timeout(10000)
local ok , err    = redis:connect("127.0.0.1", 6379)
if err then ngx.say(err) return end

local scripts = ngx.shared.scripts

local tableName = ngx.var.arg_table
local dateTime  = ngx.var.arg_datetime
local uid       = ngx.var.arg_uid

local dimsStr   = ngx.var.arg_dims
local dims      = ngx_re.split(dimsStr , ",")

local r = redis:evalsha(scripts:get("WriteScript") , 4 , tableName , dateTime , uid , 0 , unpack(dims))
ngx.say(r)
redis:set_keepalive(10000, 100)